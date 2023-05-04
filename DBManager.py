import psycopg2


class DBManager:
    #def __init__(self, dbname):
     #   self.dbname = dbname

    def create_database(self):
        """
        Создает новую БД
        """
        conn = psycopg2.connect(user="postgres",
                                password="17122003M",
                                host="localhost",
                                port="5432"
                                )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute('DROP DATABASE IF EXISTS HeadHunter')
        cur.execute('CREATE DATABASE HeadHunter')
        cur.close()
        conn.close()

    def create_tables(self):
        """
        Создает таблицы в бд
        """
        conn = psycopg2.connect(
                                host="localhost",
                                database="headhunter",
                                user="postgres",
                                password="17122003M"
                                )
        with conn:
            with conn.cursor() as cur:
                cur.execute('CREATE TABLE employers'
                            '('
                            'employer_id int PRIMARY KEY, '
                            'employer_name varchar(255) UNIQUE NOT NULL,'
                            'employer_url text UNIQUE NOT NULL)')
                cur.execute('CREATE TABLE vacancies '
                            '('
                            'vacancy_id int PRIMARY KEY, '
                            'vacancy_name varchar(255) NOT NULL, '
                            'employer_id int REFERENCES employers(employer_id) NOT NULL, '
                            'description text,'
                            'url text, '
                            'payment_from int NULL,'
                            'payment_to int NULL,'
                            'date_published date)')
        conn.close()

    def insert_in_employers(self, employer_id, employer, employer_url) -> None:
        """
        Добавлеяет данные
        в бд в зависимости
        от таблицы
        """
        conn = psycopg2.connect(host="localhost",
                                database="headhunter",
                                user="postgres",
                                password="17122003M"
                                )

        with conn:
            with conn.cursor() as cur:
                cur.execute('INSERT INTO employers(employer_id, employer_name, employer_url) '
                            'VALUES(%s, %s, %s)'
                            'ON CONFLICT (employer_id) DO NOTHING', (employer_id, employer, employer_url))

        cur.close()
        conn.close()

    def insert_in_vacancies(self, vacancy_id, vacancy_name, employer_id, description, url, payment_from, payment_to,
                            date_published) -> None:
        """
        Добавлеяет данные
        в бд в зависимости
        от таблицы
        """
        conn = psycopg2.connect(host="localhost",
                                database="headhunter",
                                user="postgres",
                                password="17122003M"
                                )

        with conn:
            with conn.cursor() as cur:
                cur.execute('INSERT INTO vacancies(vacancy_id, vacancy_name, employer_id, '
                            'description, url, payment_from, payment_to, date_published) '
                            'VALUES(%s, %s, %s, %s, %s, %s, %s, %s)'
                            'ON CONFLICT (vacancy_id) DO NOTHING', (vacancy_id, vacancy_name, employer_id, description, url,
                             payment_from, payment_to, date_published))

        cur.close()
        conn.close()

    def execute_query(self, query) -> list:
        """
        Возвращает
        результат запроса
        """
        conn = psycopg2.connect(host="localhost",
                                database="headhunter",
                                user="postgres",
                                password="17122003M"
                                )

        with conn:
            with conn.cursor() as cur:
                cur.execute(query)
                result = cur.fetchall()

        conn.close()

        return result

    def get_companies_and_vacancies_count(self) -> list:
        """
        Получает список всех компаний
        и количество вакансий у каждой компании
        """
        result = self.execute_query("SELECT employer_name, COUNT(*) as number_of_vacancies "
                                    "FROM vacancies "
                                    "LEFT JOIN employers USING(employer_id)"
                                    "GROUP BY employer_name "
                                    "ORDER BY number_of_vacancies DESC, employer_name")
        return result

    def get_all_vacancies(self) -> list:
        """
        Получает список всех вакансий
        с указанием названия компании,
        названия вакансии и зарплаты
        и ссылки на вакансию
        """
        result = self.execute_query("SELECT employers.employer_name, vacancy_name, payment_from, payment_to, url "
                                    "FROM vacancies "
                                    "JOIN employers USING(employer_id)"
                                    "WHERE payment_from IS NOT NULL AND payment_to IS NOT NULL "
                                    "ORDER BY payment_from DESC, vacancy_name")
        return result

    def get_avg_salary(self) -> list:
        """
        Получает среднюю
        зарплату по вакансиям
        """
        result = self.execute_query("SELECT ROUND(AVG(payment_from)) as average_salary "
                                    "FROM vacancies")
        return result

    def get_vacancies_with_higher_salary(self) -> list:
        """
        Получает список всех вакансий,
        у которых зарплата выше средней
        по всем вакансиям
        """
        result = self.execute_query("SELECT vacancy_name, payment_from "
                                    "FROM vacancies "
                                    "WHERE payment_from > (SELECT AVG(payment_from) FROM vacancies) "
                                    "ORDER BY payment_from DESC, vacancy_name")
        return result

    def get_vacancies_with_keyword(self, word: str) -> list:
        """
        Получает список всех вакансий,
        в названии которых содержатся переданные
        в метод слова
        """
        result = self.execute_query("SELECT vacancy_name "
                                    "FROM vacancies "
                                    f"WHERE vacancy_name ILIKE '%{word}%'"
                                    "ORDER BY vacancy_name")
        return result
