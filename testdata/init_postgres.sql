CREATE TABLE balances (
  id BIGSERIAL PRIMARY KEY,
  amount BIGINT NOT NULL
);

INSERT INTO balances (amount) VALUES (100);
