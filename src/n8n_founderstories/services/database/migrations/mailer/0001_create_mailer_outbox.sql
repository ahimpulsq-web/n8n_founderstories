CREATE TABLE IF NOT EXISTS public.mailer_outbox (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),

  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),

  request_id text NOT NULL,
  master_result_id uuid NOT NULL,

  organization text,

  domain text,

  company_name text,
  company_conf text,
  company_source_link text,

  contact_names text,

  email_id text,
  email_conf text,
  email_source_link text,


  subject text,
  content text,

  mail_status text NOT NULL DEFAULT 'PENDING',
  send_status text NOT NULL DEFAULT 'NOT_SENT',

  notes text,

  CONSTRAINT uq_mailer_outbox_req_master
    UNIQUE (request_id, master_result_id)
);

CREATE INDEX IF NOT EXISTS idx_mailer_outbox_request_id
  ON public.mailer_outbox (request_id);

CREATE INDEX IF NOT EXISTS idx_mailer_outbox_send_status
  ON public.mailer_outbox (send_status);

CREATE INDEX IF NOT EXISTS idx_mailer_outbox_mail_status
  ON public.mailer_outbox (mail_status);
