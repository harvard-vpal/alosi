import numpy as np
from alosi.engine import BaseAdaptiveEngine


class PrototypeAdaptiveEngine(BaseAdaptiveEngine):
    """
    Example demonstrating the subclassing of BaseAdaptiveEngine to implement an adaptive engine
    """
    def __init__(self, W_p, W_r, W_d, W_c):
        """
        Accepts weights W_p, W_r, W_d, W_c as input
        """
        # placeholder data
        self.Scores = np.array([
            [1, 1, 0.5],
            [1, 2, 0.9],
            [2, 1, 1.0],
        ])
        self.Mastery = np.array([
            [0.1, 0.2],
            [0.3, 0.5],
        ])
        self.MasteryPrior = np.array([0.1, 0.1])
        self.Guess = np.array([
            [0.1, 0.2],
            [0.3, 0.4],
            [0.5, 0.6]
        ])
        self.Slip = np.array([
            [0.1, 0.2],
            [0.3, 0.4],
            [0.5, 0.6]
        ])
        self.Transit = np.array([
            [0.1, 0.2],
            [0.3, 0.4],
            [0.5, 0.6]
        ])
        self.r_star = 0.0
        self.L_star = 2.2
        self.W_p = W_p
        self.W_r = W_r
        self.W_d = W_d
        self.W_c = W_c

    def get_guess(self, activity=None):
        if activity is not None:
            return self.Guess[activity]
        else:
            return self.Guess

    def get_slip(self, activity=None):
        if activity is not None:
            return self.Slip[activity]
        else:
            return self.Slip

    def get_transit(self, activity=None):
        if activity is not None:
            return self.Transit[activity]
        else:
            return self.Transit

    def get_difficulty(self):
        return np.array([0.1, 0.5, 0.9])

    def get_prereqs(self):
        return np.array([
            [0, 1],
            [0, 0]
        ])

    def get_r_star(self):
        return self.r_star

    def get_L_star(self):
        return self.L_star

    def get_last_attempted_guess(self, learner):
        # placeholder
        return np.array([0.5, 0.3])

    def get_last_attempted_slip(self, learner):
        # placeholder
        return np.array([0.5, 0.4])

    def get_learner_mastery(self, learner):
        # placeholder
        return np.log([0.5, 0.7])

    def get_mastery_prior(self):
        return self.MasteryPrior

    def get_W_p(self):
        return self.W_p

    def get_W_r(self):
        return self.W_r

    def get_W_d(self):
        return self.W_d

    def get_W_c(self):
        return self.W_c

    def get_scores(self):
        return self.Scores

    def save_score(self, learner, activity, score):
        self.Scores = np.vstack((self.Scores, [learner, activity, score]))

    def update_learner_mastery(self, learner, new_mastery):
        self.Mastery[learner] = new_mastery

    def update_guess(self, new_matrix):
        self.Guess = new_matrix

    def update_slip(self, new_matrix):
        self.Slip = new_matrix

    def update_transit(self, new_matrix):
        self.Transit = new_matrix


# example usage of subclass instance

# create a subclass instance
engine = PrototypeAdaptiveEngine(5.0, 3.0, 1.0, 0.5)

# use the recommend method
engine.recommend(learner=1)

# use the bayesian update method based on a score
engine.update_from_score(learner=0, activity=0, score=0.5)

# re-estimate the model parameters
engine.train()
