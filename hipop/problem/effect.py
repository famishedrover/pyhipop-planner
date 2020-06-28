from typing import Set, Tuple
import logging
LOGGER = logging.getLogger(__name__)


class Effect:
    def __init__(self, positive_condition=frozenset(),
                 negative_condition=frozenset(),
                 add_literals=frozenset(), del_literals=frozenset()):
        self.__pos = positive_condition
        self.__neg = negative_condition
        self.__add = add_literals
        self.__del = del_literals

    def applicable(self, state: Set[str]) -> Tuple[Set[str], Set[str]]:
        """Returns applicable effects."""
        if (self.__pos <= state and self.__neg.isdisjoint(state)):
            return self.__add, self.__del
        return frozenset(), frozenset()

    @property
    def conditions(self) -> Tuple[Set[str], Set[str]]:
        """Return the conditions"""
        return self.__pos,self.__neg

    @property
    def adds(self) -> frozenset():
        return self.__add

    @property
    def dels(self) -> frozenset():
        return self.__del

    def __repr__(self) -> str:
        if self.__pos or self.__neg:
            return f"when {self.__pos} and not {self.__neg}: add {self.__add} and del {self.__del}"
        else:
            return f"add {self.__add} and del {self.__del}"

'''
    def apply(self, state: Set[str]) -> Set[str]:
        """Apply operator to state and return a new state."""
        LOGGER.debug("apply %s to %s:", repr(self), state)
        positive = set()
        negative = set()
        for eff in self.effects:
            pos, neg = eff.applicable(state)
            positive |= pos
            negative |= neg
        LOGGER.debug("literals to add: %s", positive)
        LOGGER.debug("literals to del: %s", negative)
        new_state = (state - negative) | positive
        LOGGER.debug("result in %s", new_state)
        return new_state
'''
