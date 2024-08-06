from user_interaction import (user_interaction,
                              print_get_all_vacancies,
                              print_get_companies_and_vacancies_count,
                              print_get_vacancies_with_higher_salary,
                              print_get_vacancies_with_keyword,
                              print_get_all_vacancies_with_some_columns)
from sql.db_filler import create_and_fill_tables, drop_tables


if __name__ == '__main__':
    create_and_fill_tables()
    user_interaction()
    drop_tables()
