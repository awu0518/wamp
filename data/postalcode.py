from abc import ABC, abstractmethod
import re


class PostalCode(ABC):
    @abstractmethod
    def __init__(self, code: str):
        print("Can't init this class!")

    def __str__(self):
        return self.code


US_POSTCODE_LEN = 5  


class USPostalCode(PostalCode):
    def __init__(self, code: str):
        if not isinstance(code, str):
            raise TypeError(f'Bad type for code: {type(code)}')
        if len(code) != US_POSTCODE_LEN:
            raise ValueError(f'Bad value for {code=}')
        if not code.isdigit():
            raise ValueError(f'Only numbers allowed in {code=}')
        self.code = code

    def get_code(self) -> str:
        return self.code


MIN_UK_POSTCODE_LEN = 7
MAX_UK_POSTCODE_LEN = 9
TEST_UK_CODE = 'LO45 56HA'
TEST_UK_CODE_SHORT = 'L42 6HA'
TEST_UK_CODE_LOWER = 'lo45 56ha'


class UKPostalCode(PostalCode):
    def __init__(self, code: str):
        if not isinstance(code, str):
            raise TypeError(f'Bad type for code: {type(code)}')
        if len(code) < MIN_UK_POSTCODE_LEN or len(code) > MAX_UK_POSTCODE_LEN:
            raise ValueError(f'Bad length for {code=}')
        upper_code = code.upper()
        if re.match('[A-Z]+[0-9]+ [0-9]+[A-Z]+', upper_code):
            self.code = upper_code
        else:
            raise ValueError(f'{code=} does not match UK pattern')


def main():
    us_code = USPostalCode('99999')
    print(us_code)


if __name__ == '__main__':
    main()
