DROP DATABASE IF EXISTS HeadHunter
CREATE DATABASE HeadHunter

CREATE TABLE employers
(
    employer_id int PRIMARY KEY,
    employer_name varchar(255) UNIQUE NOT NULL,
    employer_url text UNIQUE NOT NULL
)


CREATE TABLE vacancies
(
    vacancy_id int PRIMARY KEY,
    vacancy_name varchar(255) NOT NULL,
    employer_id int REFERENCES employers(employer_id) NOT NULL,
    description text,
    url text,
    payment_from int NULL,
    payment_to int NULL,
    date_published date
)


INSERT INTO employers(employer_id, employer_name, employer_url)
VALUES(%s, %s, %s)
ON CONFLICT (employer_id) DO NOTHING, (employer_id, employer, employer_url)


INSERT INTO vacancies(vacancy_id, vacancy_name, employer_id,
description, url, payment_from, payment_to, date_published)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (vacancy_id) DO NOTHING, (vacancy_id, vacancy_name, employer_id, description, url,
payment_from, payment_to, date_published)


SELECT employer_name, COUNT(*) as number_of_vacancies
FROM vacancies
LEFT JOIN employers USING(employer_id)
GROUP BY employer_name
ORDER BY number_of_vacancies DESC, employer_name


SELECT employers.employer_name, vacancy_name, payment_from, payment_to, url
FROM vacancies
JOIN employers USING(employer_id)
WHERE payment_from IS NOT NULL AND payment_to IS NOT NULL
ORDER BY payment_from DESC, vacancy_name


SELECT ROUND(AVG(payment_from)) as average_salary
FROM vacancies


SELECT vacancy_name, payment_from
FROM vacancies
WHERE payment_from > (SELECT AVG(payment_from) FROM vacancies)
ORDER BY payment_from DESC, vacancy_name


SELECT vacancy_name
FROM vacancies
WHERE vacancy_name ILIKE %{word}%
ORDER BY vacancy_name