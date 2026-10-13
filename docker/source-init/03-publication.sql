-- Explicit administrative action: PGSync never creates publications on its own
-- unless safety.auto_create_publication is enabled, and never on production.
CREATE PUBLICATION pgsync_demo FOR TABLE public.users, public.orders;
