-- Deterministic seed data. Row counts are small on purpose: the baseline e2e test
-- snapshots this data, so it must load in a couple of seconds.

INSERT INTO app.customers (id, name, email, signup_at, lifetime_value, is_active, prefs, tags)
VALUES
    (1, 'Ada Lovelace',    'ada@example.com',    '2026-01-05T09:00:00Z', 1250.5000, true,
     '{"newsletter": true, "tier": "gold"}',        ARRAY['vip', 'early-adopter']),
    (2, 'Grace Hopper',    'grace@example.com',   '2026-02-11T14:30:00Z',  980.0000, true,
     '{"newsletter": false, "tier": "silver"}',     ARRAY['beta']),
    (3, 'Alan Turing',     'alan@example.com',    '2026-03-02T08:15:00Z',    0.0000, false,
     '{"tier": "bronze", "flags": ["dormant"]}',    ARRAY[]::text[]),
    (4, 'Katherine Johnson','katherine@example.com','2026-04-19T21:45:00Z', 4310.7500, true,
     '{"newsletter": true, "tier": "platinum"}',    ARRAY['vip']),
    (5, 'Barbara Liskov',  'barbara@example.com', '2026-05-23T11:05:00Z',  220.2500, true,
     '{}',                                          ARRAY['new']);
SELECT setval(pg_get_serial_sequence('app.customers', 'id'), 1000, false);

INSERT INTO app.orders (id, customer_id, placed_at, status, total_amount, line_items, quantities, note)
VALUES
    (1, 1, '2026-06-01T10:00:00Z', 'paid',      120.00, '[{"sku":"A-1","qty":2}]',                ARRAY[2],    'gift wrap'),
    (2, 1, '2026-06-15T16:20:00Z', 'shipped',    45.99, '[{"sku":"B-7","qty":1}]',                ARRAY[1],    NULL),
    (3, 2, '2026-06-20T12:00:00Z', 'pending',   310.10, '[{"sku":"C-3","qty":5},{"sku":"A-1","qty":1}]', ARRAY[5,1], NULL),
    (4, 4, '2026-07-02T09:30:00Z', 'paid',     1999.99, '[{"sku":"Z-9","qty":1}]',                ARRAY[1],    'expedite'),
    (5, 5, '2026-07-10T18:45:00Z', 'cancelled',  15.00, '[]',                                     ARRAY[]::int[], 'customer changed mind');
SELECT setval(pg_get_serial_sequence('app.orders', 'id'), 1000, false);

INSERT INTO app.sensor_readings (sensor_id, reading_at, value, unit, meta)
VALUES
    ('sensor-a', '2026-07-01T00:00:00Z', 21.5, 'C', '{"site":"hq"}'),
    ('sensor-a', '2026-07-01T00:05:00Z', 21.7, 'C', '{"site":"hq"}'),
    ('sensor-b', '2026-07-01T00:00:00Z', 55.2, '%', '{"site":"warehouse"}'),
    ('sensor-b', '2026-07-01T00:05:00Z', 54.9, '%', '{"site":"warehouse"}');

-- One small doc and one comfortably TOASTed doc (~64 kB of highly repetitive text
-- would compress out of TOAST, so we use random-ish content).
INSERT INTO app.documents (id, title, body, body_bytes, revision)
SELECT 1, 'short-note', 'a quick note that stays inline', 30, 1;
INSERT INTO app.documents (id, title, body, body_bytes, revision)
SELECT 2,
       'toasted-doc',
       string_agg(md5(g::text), '')          -- 32 chars * 2000 = 64000 bytes, incompressible
       , 64000, 1
FROM generate_series(1, 2000) g;
SELECT setval(pg_get_serial_sequence('app.documents', 'id'), 1000, false);

INSERT INTO app.wide_types VALUES (
    1,
    32767,                                  -- smallint
    2147483647,                             -- integer
    9223372036854775807,                    -- bigint
    3.4028235e38,                           -- real
    1.7976931348623157e308,                 -- double
    12345678901234.1234567890,              -- numeric(30,10)
    'NaN'::numeric,                         -- numeric NaN
    true,
    'fixed   ',
    'variable length',
    'plain text',
    '\xdeadbeef'::bytea,
    '2026-07-30',
    '13:45:56.123456',
    '13:45:56.123456+02',
    '2026-07-30T13:45:56.123456',
    '2026-07-30T13:45:56.123456Z',
    '3 days 04:05:06',
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
    '{"kind":"json","n":1}',
    '{"kind":"jsonb","nested":{"a":[1,2,3]}}',
    ARRAY[1, 2, 3],
    ARRAY['x', 'y', 'z'],
    ARRAY[1.10, 2.20]::numeric(12,2)[],
    NULL,                                   -- inet: stock text adapter is not literal ::text
    '10.0.0.0/8',
    '08:00:2b:01:02:03',
    NULL,                                   -- money: stock text adapter is not literal ::text
    B'10101010',
    'shipped',
    '(1.5,2.5)',
    '[1,10)',
    'Infinity'::double precision,
    'NaN'::double precision
);

INSERT INTO app.audit_log (id, occurred_at, actor, action, payload)
VALUES
    (1, '2026-06-05T10:00:00Z', 'ada',   'login',  '{"ip":"10.0.0.1"}'),
    (2, '2026-07-05T10:00:00Z', 'grace', 'update', '{"table":"orders"}'),
    (3, '2026-07-25T10:00:00Z', 'alan',  'delete', '{"table":"customers"}');
SELECT setval(pg_get_serial_sequence('app.audit_log', 'id'), 1000, false);
