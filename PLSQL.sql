/* 1 CUSTOMER_INFO- Müştəri məlumatlarını özündə saxlayan table */

CREATE TABLE customer_info (cif NUMBER PRIMARY KEY,
                            NAME VARCHAR2(20),
                            surname VARCHAR2(30),
                            gender VARCHAR2(3),
                            age NUMBER,
                            job VARCHAR2(30),
                            marital VARCHAR2(20),
                            education VARCHAR2(20),
                            country VARCHAR2(30));

SELECT * FROM customer_info FOR UPDATE;


/* 2 Customer_churn_info- adlı table yaratmaq. Datani For Update ilə Table-ə daxil etmək 
Bank churn etmiş olan (yəni bankı tərk etmiş olan müştərilər üçün müəyyən kampaniya, 
təkliflər göndərmək və churn rate(faiz) aşağı salmağa çalışır onlara kreditvermə şərtləri 
üzərində işləmək istəyir) */ 

CREATE TABLE customer_churn_info (customer_id NUMBER PRIMARY KEY,
                                  credit_score NUMBER,
                                  tenure NUMBER,
                                  balance NUMBER(12,2),
                                  products_number NUMBER,
                                  credit_card NUMBER,
                                  active_member NUMBER,
                                  estimated_salary NUMBER(12,2),
                                  churn NUMBER);
                                  
SELECT * FROM customer_churn_info FOR UPDATE;


/* 3 Updated_list tablesini yaratmaq və updated_listdəki bəzi müştərilər ilə yoxlamaq əgər 
həm Customer_churn_info Tabledə  bu müştərilər varsa, updated_listdəki Balanslara və 
churn sütunlarındakı məlumatı Customer_churn_info  tablesinin müvafiq sütunlarına 
yazmaq yəni həmin sütündakı məlumatları updated_listə görə yeniləmək lazımdır. 
Updated_list –də olub Customer_churn_info  burada olmayan sətir varsa, o halda 
Customer_churn_info  tableyə insert etmək */


CREATE TABLE updated_list (customer_id NUMBER PRIMARY KEY,
                                  credit_score NUMBER,
                                  tenure NUMBER,
                                  balance NUMBER(12,2),
                                  products_number NUMBER,
                                  credit_card NUMBER,
                                  active_member NUMBER,
                                  estimated_salary NUMBER(12,2),
                                  churn NUMBER);

SELECT * FROM updated_list FOR UPDATE;

CREATE OR REPLACE PROCEDURE set_sync_customer_info IS
  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  MERGE INTO customer_churn_info cci
  USING updated_list ul
  ON (cci.customer_id = ul.customer_id)
  
  WHEN MATCHED THEN
    UPDATE SET cci.balance = ul.balance, cci.churn = ul.churn
    
  
  WHEN NOT MATCHED THEN
    INSERT
      (customer_id,
       credit_score,
       tenure,
       balance,
       products_number,
       credit_card,
       active_member,
       estimated_salary,
       churn)
    VALUES
      (ul.customer_id,
       ul.credit_score,
       ul.tenure,
       ul.balance,
       ul.products_number,
       ul.credit_card,
       ul.active_member,
       ul.estimated_salary,
       ul.churn);
  COMMIT;
END;

BEGIN
  set_sync_customer_info;
END;


/* 4 Customer_churn_info tablesinə Max_cre_amount sütunu əlavə etmək(bu sütun bir 
müştəriyə verilə biləcək maksimum kredit məbləğini təyin edir 
İlk əvvəl müxtəlif statistikalar aparaq. */

ALTER TABLE customer_churn_info
ADD max_cre_amount NUMBER;


/* 5 Ən çox churn edən müştərilərin hansı cinsdən olduğunu təyin etmək. */

--1 → müştəri churn edib 
--0 → müştəri churn etməyib

CREATE OR REPLACE FUNCTION get_churn_count
  RETURN SYS_REFCURSOR IS
  count_of_churn SYS_REFCURSOR;
BEGIN
  OPEN count_of_churn FOR
    SELECT ci.gender, COUNT(*) AS churn_count
      FROM customer_churn_info cci
      JOIN customer_info ci
        ON ci.cif = cci.customer_id
     WHERE churn = 1
     GROUP BY ci.gender
     ORDER BY COUNT(*) DESC
     FETCH FIRST 1 ROWS ONLY;
  RETURN count_of_churn;
END;

DECLARE
     count_of_churn SYS_REFCURSOR;
     v_gender customer_info.gender%TYPE;
     v_count NUMBER;
BEGIN
    count_of_churn := get_churn_count;
    LOOP
       FETCH count_of_churn 
       INTO v_gender, v_count;
       EXIT WHEN count_of_churn%NOTFOUND;
       dbms_output.put_line('Gender: ' || v_gender || ' Count: ' || v_count);
    END LOOP;
    CLOSE count_of_churn;
END; 


/* 6 Churn etməyən müştərilər arasından və ən az maaş alanlar siyahısından ilk ən az maaş 
alan müştərini çıxmaq şərtilə növbəti 3 müştərini tapmaq. (Fetch –siz ofsetsiz 
yazılmalıdır) */

--1 → müştəri churn edib 
--0 → müştəri churn etməyib

CREATE OR REPLACE FUNCTION get_mins_salary_list
RETURN SYS_REFCURSOR
IS
  customers_churn_list SYS_REFCURSOR;
BEGIN
  OPEN customers_churn_list FOR
  SELECT customer_id,
         estimated_salary
    FROM (SELECT cci.customer_id,
                 cci.estimated_salary,
                 dense_rank() OVER (ORDER BY estimated_salary ASC) AS dr
            FROM customer_churn_info cci
           WHERE churn = 0)
   WHERE dr > 1
     AND dr <= 4;
RETURN customers_churn_list;
END;

DECLARE
  customers_churn_list SYS_REFCURSOR;
  v_customer_id customer_churn_info.customer_id%TYPE;
  v_estimated_salary customer_churn_info.estimated_salary%TYPE;
BEGIN
  customers_churn_list := get_mins_salary_list;
  LOOP
    FETCH customers_churn_list
    INTO v_customer_id,v_estimated_salary;
    EXIT WHEN customers_churn_list%NOTFOUND;
    dbms_output.put_line('Customer_id : ' || v_customer_id);
    dbms_output.put_line('Salary: ' || v_estimated_salary);
   END LOOP;
   CLOSE customers_churn_list;
END;


/* 7
Churn edən müştərilərin sayının ən çox olduğu top 10 ölkəni təyin etmək(sayı ən çox 1
dən daha artıq eyni data varsa 11-12-n kimi də götürə bilər) */

DECLARE 
  TYPE type_churn_count IS RECORD (count_of_churn NUMBER,
                                   country customer_info.country%TYPE);
  TYPE type_count_of_churn IS TABLE OF type_churn_count;
  count_of_churn type_count_of_churn;
BEGIN                                          
  SELECT
    COUNT(cci.customer_id) AS count_of_churn,
    ci.country
  BULK COLLECT INTO count_of_churn
  FROM customer_info ci JOIN customer_churn_info cci ON (ci.cif = cci.customer_id)
  WHERE churn = 1
  GROUP BY ci.country
  ORDER BY COUNT(cci.customer_id) DESC FETCH FIRST 10 ROWS WITH ties;
  
  FOR i IN count_of_churn.first .. count_of_churn.last LOOP
    dbms_output.put_line(count_of_churn(i).count_of_churn);
    dbms_output.put_line(count_of_churn(i).country);
  END LOOP;
END;

/* 8 Churn edən müştərilərin kartinin balansi ən çox olan top 10 müştərini tapmaq */

DECLARE
  TYPE type_top_customer IS RECORD (customer_id customer_churn_info.customer_id%TYPE,
                                    balance customer_churn_info.balance%TYPE);
  TYPE top_customer_list IS TABLE OF type_top_customer;
  top_list top_customer_list;
BEGIN
  SELECT
    customer_id,
    balance
  BULK COLLECT INTO top_list
    FROM (SELECT 
            customer_id,
            balance,
            dense_rank() OVER (ORDER BY balance DESC) AS dr
          FROM customer_churn_info
          WHERE churn = 1)
    WHERE dr <= 10;
    
    IF top_list.count > 0 THEN
      FOR i IN top_list.first .. top_list.last LOOP
        dbms_output.put_line(' Customer: ' || top_list(i).customer_id || 
                             ' Balance: ' || top_list(i).balance);
        END LOOP;
    END IF;
END;



/* 9 Churn etmiş müştəri yenidən bankdan kredit götürmək istəyir . Bu müştəriyə kreditin 
verilmə mümkünlüyünü yoxlayan bir package qurmaq 
1. Müştərinin Scoru 500-dən aşağıdırsa,churn edibsə və balansındakı məbləğ 2000-dən 
aşağıdırsa,ona kredit verilməyəcəkdir. Bunu təyin edən subprogram yazmaq.Müştəri 
yoxdursa exceptionda nəzərə almaq. */ 

/* 2. Autonomous transaction tətbiq etmək.  
Bu package daxilində Müştərilərin kredit müraciətlərini credit_request adlı tableda 
loglamaq lazımdır.Credit request yuxarıdakı subprogram işlədikdən sonra hansı 
müştəri, nə zaman,nə qədər məbləğdə sorğunun nəticəsi (1 –müsbət,0-mənfi yəni 
əgər ,kredit verilməyəcəksə 0  ) kredit sorğusuna müraciət edibsə onu müvafiq 
sütunlarda loglamalıdır.  */

/* 3. Kredit verilməsi mümkün olan churn etdən (etməyən) müştərilərin CİF(Customer_no) görə 
veriləcək maksimal kredit məbləğini hesablayan yeni credit_offer_amount  adlı 
funksiyasını yazmaq.Kreditin max məbləği aşağıdakı şərtlə hesablanacaqdır. 
Credit_score datası mindən kiçikdirsə, Balance məbləğinin 2 misli məbləğində 
böyükdürsə və balansindaki məbləğ 2000-dən çoxdursa, Balance məbləğinin 5 misli 
məbləğində mak kredit veriləcəkdir. */ 

/* 4. Kredit verilməsi mümkün olan churn edən müştərilərin ad soyadlarına görə veriləcək 
maksimal kredit məbləğini hesablayan yeni credit_offer_amount  adlı funksiyasını 
yazmaq.Kreditin max məbləği aşağıdakı şərtlə hesablanacaqdır. Credit_score datası 
mindən kiçikdirsə, Balance məbləğinin 1.5 misli məbləğində böyükdürsə və 
balansindaki məbləğ 2000-dən çoxdursa, Balance məbləğinin 2.5 misli məbləğində 
mak kredit veriləcəkdir.*/ 

CREATE TABLE credit_request (customer_id NUMBER, 
                             sys_date DATE, 
                             amount NUMBER (10,2),
                             credit_result NUMBER);
                             

CREATE OR REPLACE PACKAGE check_credit_list IS
  FUNCTION get_credit_list (p_customer_id customer_churn_info.customer_id%TYPE) RETURN VARCHAR2;
  PROCEDURE check_credit_request(p_customer_id IN customer_churn_info.customer_id%TYPE,
                                                                p_amount      IN NUMBER,
                                                                p_result      OUT NUMBER);
  FUNCTION credit_offer_amount (p_customer_id customer_churn_info.customer_id%TYPE) RETURN NUMBER;
  FUNCTION credit_offer_amount(p_name customer_info.name%TYPE,
                               p_surname customer_info.surname%TYPE)
  RETURN NUMBER;
  
END;
  
CREATE OR REPLACE PACKAGE BODY check_credit_list IS

FUNCTION get_credit_list (p_customer_id customer_churn_info.customer_id%TYPE) RETURN VARCHAR2
IS
customer_data customer_churn_info%ROWTYPE;
BEGIN
  SELECT *
  INTO customer_data
  FROM customer_churn_info
  WHERE customer_id = p_customer_id;
  
  IF customer_data.credit_score < 500
    AND customer_data.churn = 1 
    AND customer_data.balance < 2000 THEN
    RETURN 'Kredit verilmir';
  ELSE
    RETURN 'Kredit verile biler';
  END IF;
  
  EXCEPTION
    WHEN no_data_found THEN
      RETURN 'Musteri tapilmadi';
    WHEN OTHERS THEN
      RETURN 'xeta bas verdi';      
END get_credit_list;


PROCEDURE check_credit_request(p_customer_id IN customer_churn_info.customer_id%TYPE,
                                                                p_amount      IN NUMBER,
                                                                p_result      OUT NUMBER) IS
  PRAGMA AUTONOMOUS_TRANSACTION;
  v_customer_info customer_churn_info%ROWTYPE;
  customer_not_found EXCEPTION;
BEGIN
  SELECT *
    INTO v_customer_info
    FROM customer_churn_info
   WHERE customer_id = p_customer_id;

  IF v_customer_info.credit_score < 500 AND v_customer_info.churn = 1 AND
     v_customer_info.balance < 200 THEN
    p_result := 0;
  ELSE
    p_result := 1;
  END IF;

  INSERT INTO credit_request
    (customer_id, sys_date, amount, credit_result)
  VALUES
    (p_customer_id, SYSDATE, p_amount, p_result);
    
  IF v_customer_info.customer_id IS NULL THEN
    RAISE customer_not_found;
  END IF;
  COMMIT;
  
EXCEPTION
  WHEN customer_not_found THEN
    dbms_output.put_line ('Customer not found');
END check_credit_request;


FUNCTION credit_offer_amount (p_customer_id customer_churn_info.customer_id%TYPE)
  RETURN NUMBER
IS
  v_credit_score customer_churn_info.credit_score%TYPE;
  v_balance customer_churn_info.balance%TYPE;
  v_churn customer_churn_info.churn%TYPE;
BEGIN
  SELECT credit_score, balance, churn
  INTO v_credit_score, v_balance, v_churn
  FROM customer_churn_info
  WHERE customer_id = p_customer_id;
  
  IF v_churn = 1 THEN
    RETURN 0;
  END IF;
  
  IF v_credit_score < 1000 THEN
    RETURN v_balance * 2;
  ELSIF v_credit_score > 1000 AND v_balance > 2000 THEN
    RETURN v_balance * 5;
  END IF;
RETURN 0;

EXCEPTION
  WHEN no_data_found THEN
    RETURN 0;
END credit_offer_amount;


FUNCTION credit_offer_amount(p_name customer_info.name%TYPE,
                             p_surname customer_info.surname%TYPE)
  RETURN NUMBER 
  IS
  v_credit_score customer_churn_info.credit_score%TYPE;
  v_balance      customer_churn_info.balance%TYPE;
  v_churn        customer_churn_info.churn%TYPE;
BEGIN
  SELECT cci.credit_score, cci.balance, cci.churn
    INTO v_credit_score, v_balance, v_churn
    FROM customer_churn_info cci
    JOIN customer_info ci
      ON cci.customer_id = ci.cif
   WHERE ci.name = p_name
         AND ci.surname = p_surname 
         AND cci.churn = 1;
         
  IF v_churn = 0 THEN
    RETURN 0;
  END IF;

  IF v_credit_score < 1000 THEN
    RETURN v_balance * 1.5;
  ELSIF v_credit_score > 1000 AND v_balance > 2000 THEN
    RETURN v_balance * 2.5;
  END IF;

EXCEPTION
  WHEN no_data_found THEN
    RETURN 0;
END credit_offer_amount;

END check_credit_list;

--1 function
SELECT
  check_credit_list.get_credit_list(1156496)
FROM dual;

--2 procedure
DECLARE
  v_result NUMBER;
BEGIN
  check_credit_list.check_credit_request(p_customer_id => 2365987,
                       p_amount      => 10000,
                       p_result      => v_result);
  dbms_output.put_line(v_result);
END;

--3 function
DECLARE
  v_number NUMBER;
BEGIN
  v_number := check_credit_list.credit_offer_amount(p_customer_id => 1155981);
  dbms_output.put_line('Max Credit amount: ' || v_number);
END;

--4
DECLARE
  v_number NUMBER;
BEGIN
  v_number := check_credit_list.credit_offer_amount(p_name =>'Elia', 
                                                    p_surname => 'Fawcett');
  dbms_output.put_line('Max Credit amount: ' || v_number);
END;


/* 5. Job vasitəsilə hər ayın ilk günü(yəni job hər ayın ilk günü işləyəcək) bütün müştərilər 
üçün credit_offer_amount   funksiyasının qaytardığı nəticə cre_max_amount 
sütunundakı datadan fərqlidirsə, həmin sütunu update edib funksiyanın nəticəsini 
yazmaq (ixtiyari müştərilər üzrə) */


UPDATE customer_churn_info
SET max_cre_amount = check_credit_list.credit_offer_amount(customer_id)
WHERE NVL(max_cre_amount,0) <> check_credit_list.credit_offer_amount(customer_id);

BEGIN
  DBMS_SCHEDULER.CREATE_JOB (
   job_name        => 'update_data',
   job_type        => 'plsql_block',
   job_action      => q'[BEGIN UPDATE customer_churn_info
                            SET max_cre_amount = check_credit_list.credit_offer_amount(customer_id)
                            WHERE NVL(max_cre_amount,0) <> check_credit_list.credit_offer_amount(customer_id); 
                        COMMIT;
                        END;]',
   repeat_interval => 'FREQ=MONTHLY;BYMONTHDAY=1',
   enabled         => TRUE,
   auto_drop       => FALSE);
END;
