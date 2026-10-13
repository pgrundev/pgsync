-- Synthetic demo dataset. No real data.
CREATE TABLE public.users (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email       text NOT NULL UNIQUE,
    full_name   varchar(120),
    is_active   boolean NOT NULL DEFAULT true,
    balance     numeric(12, 2) NOT NULL DEFAULT 0,
    external_id uuid NOT NULL DEFAULT gen_random_uuid(),
    profile     jsonb,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE public.orders (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id     bigint NOT NULL REFERENCES public.users (id),
    status      text NOT NULL DEFAULT 'pending',
    total       numeric(12, 2) NOT NULL,
    items       jsonb NOT NULL DEFAULT '[]'::jsonb,
    note        text,
    payload     bytea,
    placed_at   timestamp NOT NULL DEFAULT now(),
    updated_at  timestamptz
);

INSERT INTO public.users (email, full_name, balance, profile)
SELECT format('user%s@example.test', g),
       format('User %s', g),
       (g * 37 % 1000)::numeric / 10,
       jsonb_build_object('tier', CASE WHEN g % 3 = 0 THEN 'gold' ELSE 'basic' END, 'n', g)
FROM generate_series(1, 200) AS g;

INSERT INTO public.orders (user_id, status, total, items, note)
SELECT (g % 200) + 1,
       (ARRAY['pending', 'paid', 'shipped'])[(g % 3) + 1],
       (g * 13 % 500)::numeric + 0.99,
       jsonb_build_array(jsonb_build_object('sku', format('SKU-%s', g % 50), 'qty', (g % 4) + 1)),
       CASE WHEN g % 10 = 0 THEN NULL ELSE format('note %s', g) END
FROM generate_series(1, 1000) AS g;

GRANT USAGE ON SCHEMA public TO pgsync;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO pgsync;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO pgsync;
