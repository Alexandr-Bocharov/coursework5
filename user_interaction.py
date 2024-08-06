from src.hh_vacancies import HeadHunterAPIVacancies
from sql.db_manager import DBManager
from sql.db_filler import drop_tables


def user_interaction():
    """ Функция для взаимодействия с пользователем """
    db_manager = DBManager('coursework5', 'postgres', '2306', 'localhost', '5433')

    print('Добро пожаловать!')

    while True:
        number = input('''Введите одну из следующих цифр:
1 - Вывести список всех компаний и количество вакансий у каждой компании
2 - Вывести список всех вакансий c колонками: компания, название вакансии, зарплата, ссылка на вакансию
3 - Вывести список всех вакансий со всеми колонками
4 - Вывести вакансии, у которых зарплата выше средней по всем вакансиям
5 - Вывести вакансии по определенному слову
6 - Выход\n''')

        if number == '1':
            result = db_manager.get_companies_and_vacancies_count()
            print_get_companies_and_vacancies_count(result)
        elif number == '2':
            result = db_manager.get_all_vacancies_with_some_columns()
            print_get_all_vacancies_with_some_columns(result)
        elif number == '3':
            result = db_manager.get_all_vacancies()
            print_get_all_vacancies(result)
        elif number == '4':
            result = db_manager.get_vacancies_with_higher_salary()
            print_get_vacancies_with_higher_salary(result)
        elif number == '5':
            keywrd = input('Введите ключевое слово для поиска:\n')
            result = db_manager.get_vacancies_with_keyword(keywrd)
            print_get_vacancies_with_keyword(result)
        elif number == '6':
            break
        elif number.isdigit():
            print(f'Введена неправильная(ое) цифра/число, попробуйте еще раз\n')
        elif not number.isdigit():
            print('Введите цифру от 1 до 6\n')

def print_get_companies_and_vacancies_count(data):
    print('-' * 100)
    for line in data:
        print(f"{line[0]}: {line[1]}")
    print('-' * 100)


def print_get_all_vacancies_with_some_columns(data):
    print('-' * 100)
    for line in data:
        print(f'Компания: {line[0]}')
        print(f"Вакансия и ссылка: {line[1]}, {line[-1]}")
        print(f"Зарплата: от {line[2]} до {line[3]} {line[4]}")
        print()
    print('-' * 100)


def print_get_all_vacancies(data):
    print('-' * 100)
    for line in data:
        print(f'id и название компании: {line[0]}, {line[1]}')
        print(f'Ссылки на компанию: {line[2]}, {line[3]}')
        print(f'id и название вакансии: {line[4]}, {line[5]}')
        print(f'Ссылки на вакансию: {line[6]}, {line[7]}')
        print(f'Опыт: {line[8]}')
        print(f'Город: {line[9]}')
        print(f'Зарплата: от {line[12]} до {line[13]} {line[14]}')
        print()
    print('-' * 100)


def print_get_vacancies_with_higher_salary(data):
    print('-' * 100)
    for line in data:
        print(f"Вакансия и ссылка: {line[1]}, {line[3]}")
        print(f"Зарплата: от {line[8]} до {line[9]} {line[10]}")
        print(f"Город: {line[5]}")
        print(f"Опыт: {line[4]}")
        print()
    print('-' * 100)


def print_get_vacancies_with_keyword(data):
    print('-' * 100)
    for line in data:
        print(f"Вакансия и ссылка: {line[1]}, {line[3]}")
        print(f"Зарплата: от {line[8]} до {line[9]} {line[10]}")
        print(f"Город: {line[5]}")
        print(f"Опыт: {line[4]}")
        print()
    print('-' * 100)





