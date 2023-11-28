import datetime

import requests


def get_person(person_id):
    1
    return requests.get(f"https://swapi.py4e.com/api/people/{person_id}/").json()


def main():
    person_1 = get_person(1)
    person_2 = get_person(2)
    person_3 = get_person(3)
    person_4 = get_person(4)

    print(person_1, person_2, person_3, person_4)


if __name__ == "__main__":
    start = datetime.datetime.now()
    main()
    print(datetime.datetime.now() - start)
