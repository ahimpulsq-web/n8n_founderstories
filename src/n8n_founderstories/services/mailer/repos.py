from psycopg import Connection
from psycopg.rows import dict_row



def seed_mailer_outbox(conn: Connection, request_id: str) -> int:
    sql = """
    INSERT INTO public.mailer_outbox (
      request_id,
      master_result_id,
      organization,
      domain,

      company_name,
      company_conf,
      company_source_link,

      contact_names,

      short_description,
      long_description,

      email_id,
      email_conf,
      email_source_link
    )
    SELECT
      r.request_id,
      r.master_result_id,
      r.organization,
      r.domain,

      r.combined_company->>'name'       AS company_name,
      r.combined_company->>'confidence' AS company_conf,
      r.combined_company->'sources'->>0 AS company_source_link,

      (
        SELECT string_agg(
                 CASE
                   WHEN COALESCE(p->>'role','') <> '' THEN (p->>'name') || ' (' || (p->>'role') || ')'
                   ELSE (p->>'name')
                 END,
                 ', '
               )
        FROM jsonb_array_elements(r.combined_people) AS p
        WHERE p ? 'name'
      ) AS contact_names,

      (
        SELECT d->>'text'
        FROM jsonb_array_elements(r.combined_descriptions) AS d
        WHERE d->>'kind' = 'short'
        LIMIT 1
      ) AS short_description,

      (
        SELECT d->>'text'
        FROM jsonb_array_elements(r.combined_descriptions) AS d
        WHERE d->>'kind' = 'long'
        LIMIT 1
      ) AS long_description,

      r.combined_emails->0->>'email'                 AS email_id,
      r.combined_emails->0->>'confidence'            AS email_conf,
      r.combined_emails->0->'sources'->0->>'url'     AS email_source_link

    FROM public.web_scraper_enrichment_results r
    WHERE r.request_id = %(request_id)s
      AND r.combine_status = 'ok'
    ON CONFLICT (request_id, master_result_id)
    DO NOTHING
    RETURNING 1;
    """

    with conn.cursor() as cur:
        cur.execute(sql, {"request_id": request_id})
        return cur.rowcount


def fetch_outbox_rows_for_generation(conn: Connection, limit: int = 20) -> list[dict]:
    sql = """
    SELECT
        id,
        master_result_id,
        contact_names,
        company_name,
        short_description,
        long_description,
        domain
    FROM public.mailer_outbox

    WHERE subject IS NULL
      AND content IS NULL
    ORDER BY id
    LIMIT %(limit)s;
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, {"limit": limit})
        return cur.fetchall()
    

def update_mail_content(
    conn: Connection,
    outbox_id: int,
    subject: str,
    content: str,
) -> None:
    sql = """
    UPDATE public.mailer_outbox
    SET subject = %(subject)s,
        content = %(content)s,
        content_generated_at = now()
    WHERE id = %(id)s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, {
            "id": outbox_id,
            "subject": subject,
            "content": content,
        })