-- COPY employees TO 'C:tmpemployees_db.csv'  WITH DELIMITER ',' CSV HEADER;

CREATE OR REPLACE FUNCTION table_to_csv(path TEXT) RETURNS void 
language plpgsql  
AS $$
 declare
    statement TEXT;
 begin
 	statement := 'COPY public.job_posts' || ' TO ''' || path || '/' ||  'job_posts.csv' ||''' DELIMITER '','' CSV HEADER';
 	EXECUTE statement;
 return;  
 end;
 $$;

select table_to_csv('/tmp')