from producer.model.person import Person
from producer.model.personAddedJob import PersonAddedJob
from faker import Faker

fake = Faker()

def person_to_dict(person: Person) -> dict:
    """Convert Person object to dictionary"""
    return {
        "firstName": person.firstName,
        "lastName": person.lastName,
        "age": person.age
    }

def person_from_dict(obj, ctx=None) -> Person:
    """Convert dictionary to Person object"""
    return Person(obj["firstName"], obj["lastName"], obj.get("age", 0))

def person_job_added_to_dict(person: PersonAddedJob) -> dict:
    """Convert PersonAddedJob object to dictionary"""
    return {
        "firstName": person.firstName,
        "lastName": person.lastName,
        "job": person.job,
        "age": person.age
    }

def person_job_added_from_dict(obj, ctx=None) -> PersonAddedJob:
    """Convert dictionary to PersonAddedJob object"""
    return PersonAddedJob(obj["firstName"], obj["lastName"], obj.get("job"), obj.get("age", 0))

def generate_random_person() -> Person:
    """Generate a random person using Faker"""
    first_name = fake.first_name()
    last_name = fake.last_name()
    age = fake.random_int(min=18, max=90)
    return Person(first_name, last_name, age)
