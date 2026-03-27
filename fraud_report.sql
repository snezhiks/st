create or replace view v_clients_accounts_cards as
        select 
            c.card_num,
            c.account,
            a.valid_to as account_valid_to,
            c.create_dt,
            c.update_dt,
            a.client,
            concat(cl.last_name, ' ', cl.first_name, ' ', cl.patronymic) as full_name,
            cl.date_of_birth,
            cl.passport_num,
            cl.passport_valid_to,
            cl.phone
        from banking.cards c 
        full join banking.accounts a
        on c.account = a.account
        full join banking.clients cl
        on a.client = cl.client_id;

CREATE TABLE IF NOT EXISTS fraud_report (
  event_dt TIMESTAMP,
  passport VARCHAR(20),
  fio VARCHAR(100),
  phone VARCHAR(20),
  event_type VARCHAR(255),
  report_dt TIMESTAMP
);


INSERT INTO fraud_report
WITH
-- Просроченный паспорт
fraud_passport_expired AS (
  SELECT
    MIN(t.transaction_date) AS event_dt,
    vc.passport_num AS passport,
    vc.full_name AS fio,
    vc.phone,
    'просроченный паспорт' AS event_type,
    CURRENT_TIMESTAMP AS report_dt
  FROM banking.transactions t
  LEFT JOIN banking.v_clients_accounts_cards vc ON t.card_num = vc.card_num
  WHERE t.transaction_date::date > vc.passport_valid_to
    AND t.fraud_processed IS FALSE
  GROUP BY vc.passport_num, vc.full_name, vc.phone
),

-- Заблокированный паспорт
fraud_passport_blocked AS (
  SELECT
    MIN(t.transaction_date) AS event_dt,
    vc.passport_num AS passport,
    vc.full_name AS fio,
    vc.phone,
    'заблокированный паспорт' AS event_type,
    CURRENT_TIMESTAMP AS report_dt
  FROM banking.transactions t
  LEFT JOIN banking.v_clients_accounts_cards vc ON t.card_num = vc.card_num
  JOIN banking.v_passport_blacklist vpb ON vc.passport_num = vpb.passport
  WHERE t.transaction_date::date <= vpb.date
    AND t.fraud_processed IS FALSE
  GROUP BY vc.passport_num, vc.full_name, vc.phone
),

-- Недействующий договор
fraud_account_invalid AS (
  SELECT
    MIN(t.transaction_date) AS event_dt,
    vc.passport_num AS passport,
    vc.full_name AS fio,
    vc.phone,
    'не действующий договор' AS event_type,
    CURRENT_TIMESTAMP AS report_dt
  FROM banking.transactions t
  LEFT JOIN banking.v_clients_accounts_cards vc ON t.card_num = vc.card_num
  WHERE t.transaction_date::date > vc.account_valid_to
    AND t.fraud_processed IS FALSE
  GROUP BY vc.passport_num, vc.full_name, vc.phone
),

-- В разных городах в течение одного часа
fraud_different_cities AS (
  WITH ranked_transactions AS (
    SELECT
      vt.terminal_city,
      LAG(t.transaction_date) OVER (PARTITION BY t.card_num ORDER BY t.transaction_date) AS prev_date,
      LAG(vt.terminal_city) OVER (PARTITION BY t.card_num ORDER BY t.transaction_date) AS prev_city,
      t.transaction_date,
      vc.passport_num AS passport,
      vc.full_name AS fio,
      vc.phone
    FROM banking.transactions t
    LEFT JOIN banking.v_terminals vt ON t.terminal = vt.terminal_id
    LEFT JOIN banking.v_clients_accounts_cards vc ON t.card_num = vc.card_num
    WHERE t.fraud_processed IS FALSE
  )
  SELECT
    transaction_date AS event_dt,
    passport,
    fio,
    phone,
    'в разных городах в течение одного часа' AS event_type,
    CURRENT_TIMESTAMP AS report_dt
  FROM ranked_transactions r
  WHERE r.prev_date IS NOT NULL
    AND EXTRACT(EPOCH FROM (r.transaction_date - r.prev_date)) <= 3600
    AND r.terminal_city != r.prev_city
),

-- Попытка подбора суммы
fraud_amount_pattern AS (
  WITH ranked_transactions AS (
  SELECT
    CASE WHEN t.oper_result = 'SUCCESS' THEN 1 ELSE 0 END AS success_result,
    CASE
      WHEN (LEAD(t.amount) OVER (PARTITION BY t.card_num, oper_type ORDER BY t.transaction_date DESC)) < t.amount THEN 1
      ELSE 0
    END AS amount_sequence,
    t.transaction_date,
    t.card_num,  -- добавляем card_num в выборку
    vc.passport_num AS passport,
    vc.full_name AS fio,
    vc.phone
  FROM banking.transactions t
  LEFT JOIN banking.v_clients_accounts_cards vc ON t.card_num = vc.card_num
  WHERE t.oper_type <> 'DEPOSIT'
    AND t.fraud_processed IS FALSE
),
fraud_candidates AS (
  SELECT
    rt.*,
    SUM(rt.amount_sequence) OVER (
      PARTITION BY rt.card_num
      ORDER BY rt.transaction_date DESC
      ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING
    ) AS sum_amount_success,
    SUM(rt.success_result) OVER (
      PARTITION BY rt.card_num
      ORDER BY rt.transaction_date DESC
      ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING
    ) AS success_count,
    (rt.transaction_date - LEAD(rt.transaction_date, 3) OVER (
      PARTITION BY rt.card_num
      ORDER BY rt.transaction_date DESC
    )) AS minutes_diff
  FROM ranked_transactions rt
)
SELECT
  transaction_date AS event_dt,
  passport,
  fio,
  phone,
  '4 убывающие транзакции с одной успешной в течение 20 минут' AS event_type,
  CURRENT_TIMESTAMP AS report_dt
FROM fraud_candidates
WHERE sum_amount_success = 4
  AND minutes_diff <= INTERVAL '20 minutes'
  AND success_count = 1
  AND success_result = 1
),

-- Объединение всех типов мошенничества
fraud_types AS (
  SELECT * FROM fraud_passport_expired
  UNION ALL
  SELECT * FROM fraud_passport_blocked
  UNION ALL
  SELECT * FROM fraud_account_invalid
  UNION ALL
  SELECT * FROM fraud_different_cities
  UNION ALL
  SELECT * FROM fraud_amount_pattern
)
SELECT
  event_dt,
  passport,
  fio,
  phone,
  event_type,
  report_dt
FROM fraud_types
ORDER BY event_dt DESC, event_type;


-- отмечаем, что данные осмотрели на фрод
UPDATE transactions
SET fraud_processed = TRUE
WHERE fraud_processed IS FALSE;

