from abc import ABC, abstractmethod
import psycopg2


@abstractmethod
class DB(ABC):
    """ Абстрактный класс для сохранения данных в БД """

    def get_companies_and_vacancies_count(self):
        pass

    def get_all_vacancies_with_some_columns(self):
        pass

    def get_all_vacancies(self):
        pass

    def get_avg_salary(self):
        pass

    def get_vacancies_with_higher_salary(self):
        pass

    def get_vacancies_with_keyword(self, keyword):
        pass


class DBManager(DB):
    """ Класс для сохранения данных в БД """

    def __init__(self, db_name, user, password, host, port):
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None

    @classmethod
    def create_instance(cls, db_name, user, password, host, port):
        return cls(db_name, user, password, host, port)

    def __connect(self):
        ''' Подключаемся к бд '''
        try:
            self.conn = psycopg2.connect(
                database=self.db_name,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            print('Успешное подключение к базе данных')
        except psycopg2.Error as e:
            print(f'Ошибка при подключении к базе данных: {e}')
            self.conn=None

    def __close_connection(self):
        ''' Закрываем подключение к бд '''
        if self.conn:
            self.conn.close()
            print('Подключение к базе данных закрыто')

    def get_companies_and_vacancies_count(self):
        """ Получает список всех компаний и количество вакансий у каждой компании """
        self.__connect()
        if not self.conn:
            print('Нет подключения к бд')
            return None

        cur = self.conn.cursor()
        try:
            cur.execute(f"SELECT employer_name, COUNT(*) AS count_vacancies FROM employers "
                        f"JOIN vacancies ON employers.employer_id=vacancies.employer_id "
                        f"GROUP BY employer_name;")
            result = cur.fetchall()
            return result
        except psycopg2.Error as e:
            print(f'Произошла ошибка: {e}')
        finally:
            self.__close_connection()

    def get_all_vacancies_with_some_columns(self):
        """ Получает список всех вакансий
        с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию """
        self.__connect()
        if not self.conn:
            print('Нет подключения к бд')
            return None

        cur = self.conn.cursor()
        try:
            cur.execute(f"SELECT employers.employer_name, vacancies.name, salaries.salary_from, salaries.salary_to, salaries.currency, vacancies.url "
                        f"FROM employers "
                        f"JOIN vacancies ON employers.employer_id=vacancies.employer_id "
                        f"JOIN salaries ON salaries.vacancy_id=vacancies.id;")
            result = cur.fetchall()
            return result
        except psycopg2.Error as e:
            print(f'Произошла ошибка: {e}')
        finally:
            self.__close_connection()

    def get_all_vacancies(self):
        """ Получает список всех вакансий со всеми колонками """
        self.__connect()
        if not self.conn:
            print('Нет подключения к бд')
            return None

        cur = self.conn.cursor()
        try:
            cur.execute(
                f"SELECT * "
                f"FROM employers "
                f"JOIN vacancies ON employers.employer_id=vacancies.employer_id "
                f"JOIN salaries ON salaries.vacancy_id=vacancies.id;")
            result = cur.fetchall()
            return result
        except psycopg2.Error as e:
            print(f'Произошла ошибка: {e}')
        finally:
            self.__close_connection()

    def get_avg_salary(self):
        """ Получает среднюю зарплату по вакансиям """
        self.__connect()
        if not self.conn:
            print('Нет подключения к бд')
            return None

        cur = self.conn.cursor()
        try:
            cur.execute(f"SELECT salary_from, salary_to FROM salaries;")
            result = cur.fetchall()
            result = list(map(lambda x: x[1] if x[1] else x[0], result))
            avg_salary = round(sum(result) / len(result))
            return avg_salary
        except psycopg2.Error as e:
            print(f"Произошла ошибка: {e}")
        finally:
            self.__close_connection()

    def get_vacancies_with_higher_salary(self):
        """ Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям """
        vacancies_with_higher_salary = []
        avg_salary = self.get_avg_salary()
        all_vacancies = self.get_all_vacancies_with_some_columns()
        for vacancy in all_vacancies:
            salary_from_to = (vacancy[2], vacancy[3])
            salary = salary_from_to[1] if salary_from_to[1] else salary_from_to[0]  # Логика для сравнения зарплаты
            if salary > avg_salary:
                vacancies_with_higher_salary.append(vacancy)
        return vacancies_with_higher_salary

    def get_vacancies_with_keyword(self, keyword):
        """ Получает список всех вакансий,
        в названии которых содержатся переданные в метод слова, например python """
        all_vacancies = self.get_all_vacancies_with_some_columns()
        vacancies_with_keyword = [vac for vac in all_vacancies if keyword.lower() in vac[1].lower()]
        return vacancies_with_keyword







# if __name__ == '__main__':
#     db_worker = DBManager('coursework5', 'postgres', '2306', 'localhost', '5433')
#     for line in db_worker.get_vacancies_with_keyword('python'):
#         print(line)






