use std::iter::FromIterator;
use fst::automaton::Automaton;
use crate::SmallVec8;

pub struct MultiAutomaton<A> {
    automatons: SmallVec8<A>,
}

impl<A: Automaton> MultiAutomaton<A> {
    pub fn matchings<'a>(&'a self, states: &'a [A::State]) -> impl Iterator<Item = usize> + 'a {
        self.automatons.iter().zip(states).enumerate()
            .filter(|(_, (a, s))| a.is_match(s))
            .map(|(i, _)| i)
    }
}

impl<A: Automaton> Automaton for MultiAutomaton<A> {
    type State = SmallVec8<A::State>;

    fn start(&self) -> Self::State {
        self.automatons.iter().map(|a| a.start()).collect()
    }

    fn is_match(&self, state: &Self::State) -> bool {
        self.automatons.iter().zip(state).any(|(a, s)| a.is_match(s))
    }

    fn can_match(&self, state: &Self::State) -> bool {
        self.automatons.iter().zip(state).any(|(a, s)| a.can_match(s))
    }

    fn will_always_match(&self, state: &Self::State) -> bool {
        self.automatons.iter().zip(state).any(|(a, s)| a.will_always_match(s))
    }

    fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
        self.automatons.iter().zip(state).map(|(a, s)| a.accept(s, byte)).collect()
    }
}

impl<A> FromIterator<A> for MultiAutomaton<A> {
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        MultiAutomaton { automatons: iter.into_iter().collect() }
    }
}
