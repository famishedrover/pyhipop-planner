from .flaws import OpenLink, AbstractFlaw

class CausalLink:
    def __init__(self, support: int, open_link: OpenLink):
        self.__support = support
        self.__ol = open_link

    @property
    def support(self) -> int:
        return self.__support

    @property
    def supported(self) -> int:
        return self.__ol.step

    @property
    def atom(self) -> int:
        return self.__ol.atom

    @property
    def value(self) -> bool:
        return self.__ol.value

    @property
    def open_link(self) -> OpenLink:
        return self.__ol

    def __str__(self) -> str:
        return f"CL {self.support} -- literal {'' if self.value else 'not '}{self.atom} -- {self.supported}"

    def __bool__(self) -> bool:
        return bool(self.__ol)

    def __eq__(self, other: 'CausalLink') -> bool:
        return self.__support == other.__support and self.__ol == other.__ol


class Decomposition:
    def __init__(self, abstract_flaw: AbstractFlaw, method: str):
        self.__task = abstract_flaw
        self.__method = method

    @property
    def method(self) -> str:
        return self.__method

    @property
    def step(self) -> int:
        return self.__task.step

    @property
    def task(self) -> str:
        return self.__task.task

    @property
    def abstract_flaw(self) -> AbstractFlaw:
        return self.__task

    def __str__(self) -> str:
        return f"Task {self.step}] {self.task} -> Method {self.method}"

    def __eq__(self, other: 'Decomposition') -> bool:
        return self.__task == other.__task and self.__method == other.__method