class GroundingImpossibleError(Exception):
    def __init__(self, predicates, assignment):
        self.__predicates = predicates
        self.__assignment = assignment

    @property
    def message(self):
        return f"Grounding of {self.__predicates} impossible for {self.__assignment}"
