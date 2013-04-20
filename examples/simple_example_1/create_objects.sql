create table DEPARTEMENTS
(
  departement_id  NUMBER,
  departement_name VARCHAR2(30),
  coeficient  number(5,2)
);

create table EMPLOYEES
(
  employee_id    NUMBER,
  departement_id NUMBER,
  first_name    VARCHAR2(30),
  last_name      VARCHAR2(30),
  hire_date      DATE,
  phone_number  VARCHAR2(30),
  salary        NUMBER(12,2)
);

create or replace function salary_update(dep_id_from number,
                                        dep_id_to  number) return number is
begin
  update employees e
    set e.salary = e.salary *
                    (select coeficient
                      from departements d
                      where d.departement_id = e.departement_id)
  where e.departement_id between dep_id_from and dep_id_to;

  return 0;

exception
  when others then
    dbms_output.put_line(sqlerrm);
    return 1;
end;
