class GroundingImpossibleError(Exception):
    def __init__(self, predicates, assignment):
        self.__predicates = predicates
        self.__assignment = assignment

    @property
    def message(self):
        return f"Grounding of {self.__predicates} impossible for {self.__assignment}"


class RequirementException(Exception):
    def __init__(self, requirement):
        self.__requirement = requirement

    @property
    def message(self):
        return f"Requirement {self.__requirement}"

class RequirementNotSupported(RequirementException):
    @property
    def message(self):
        return f"{RequirementException.message(self)} not supported"


class RequirementMissing(RequirementException):
    @property
    def message(self):
        return f"{RequirementException.message(self)} missing"
