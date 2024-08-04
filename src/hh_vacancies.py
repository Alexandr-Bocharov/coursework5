from abc import ABC, abstractmethod
import requests




class ApiVacancies(ABC):
    """ Абстрактный класс для работы с API  """

    @abstractmethod
    def get_vacancies(self, keyword):
        pass


class HeadHunterAPIVacancies(ApiVacancies):
    """ Класс для работы с API HeadHunter с вакансиями """

    def __init__(self):
        """ Конструктор для HeadHunterAPI """
        self.url = 'https://api.hh.ru/vacancies'
        self.headers = {'User-Agent': 'HH-User-Agent'}
        employer_ids = ['1429999', '1035394', '3961360', '10772647', '84585',
                        '5600787', '2180', '12550', '3529', '9498120']
        self.params = {'text': '', 'employer_id': employer_ids, 'page': 0, 'per_page': 100}
        self.vacancies = []

    def get_vacancies(self, keyword):
        self.params['text'] = keyword
        while self.params.get('page') != 20:
            response = requests.get(self.url, headers=self.headers, params=self.params)
            vacancies = response.json()['items']
            self.vacancies += vacancies
            self.params['page'] += 1
        return self.vacancies
