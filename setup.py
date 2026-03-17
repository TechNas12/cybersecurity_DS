from setuptools import find_packages,setup
from typing import List

def get_requirements() -> List[str]:
    '''
        This function returns the list of requirements.
    '''
    requirement_list : List[str] = []
    try:
        with open('requirements.txt', 'r') as file:
            # Reading lines fronm the files
            lines = file.readlines()
            for line in lines:
                requirement = line.strip()
                #Ignoring the empty lines & -e.
                if requirement and requirement != '-e.':
                    requirement_list.append(requirement)
            file.close()
        return requirement_list
    
    except FileNotFoundError:
        print(f"requirement.txt not found in the root directory. ")

setup(
    name="NetworkSecurity",
    version="0.0.1",
    author="Sanket Sachin Dahotre",
    author_email="dahotresanket12@gmail.com",
    packages = find_packages(),
    install_requires=get_requirements()
)


