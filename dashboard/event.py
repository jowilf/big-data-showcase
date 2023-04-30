import random
from collections import deque
from datetime import datetime

import plotly.graph_objs as go
from plotly.graph_objs.scatter import Legendgrouptitle

PERIOD = 15


class RealTimeEvent:
    """
    Class to store and visualize real-time user behavior data.

    Attributes:
    -----------
    dates : collections.deque
        A deque of datetime objects representing the timestamp of each data point.
        The deque is initialized with a maximum length of 15 data points and will automatically discard old data
        when new data points are added.
    views : collections.deque
        A deque of integers representing the number of views at each timestamp.
        The deque is initialized with a maximum length of 15 data points and will automatically discard old data
        when new data points are added.
    purchases : collections.deque
        A deque of integers representing the number of purchases at each timestamp.
        The deque is initialized with a maximum length of 15 data points and will automatically discard old data
        when new data points are added.
    carts : collections.deque
        A deque of integers representing the number of items added to cart at each timestamp.
        The deque is initialized with a maximum length of 15 data points and will automatically discard old data
        when new data points are added.
    """

    def __init__(self) -> None:
        """
        Initializes the RealTimeEvent object with empty deques for dates, views, purchases and carts,
        and an empty deque for incoming events.
        """
        self.dates: deque[datetime] = deque(maxlen=PERIOD)
        self.views: deque[int] = deque(maxlen=PERIOD)
        self.purchases: deque[int] = deque(maxlen=PERIOD)
        self.carts: deque[int] = deque(maxlen=PERIOD)

    def append(self, data: dict):
        """
        Appends the views, purchases and carts data from the input dictionary to their respective lists,
        and appends the current datetime to the dates list. If the length of the lists exceeds 15, the oldest
        element is removed from each list.

        Parameters:
        -----------
        data : dict
            A dictionary with keys 'view', 'purchase' and 'cart', and integer values representing the
            number of views, purchases and carts in the incoming event.
        """
        self.dates.append(datetime.now())
        self.views.append(data.get("view", 0))
        self.purchases.append(data.get("purchase", 0))
        self.carts.append(data.get("cart", 0))

    def append_random(self):
        """
        Appends random data to the views, purchases and carts lists using the append method.
        Only for testing purpose
        """
        self.append(
            {
                "view": random.randint(0, 20),
                "purchase": random.randint(0, 20),
                "cart": random.randint(0, 20),
            }
        )

    def figure(self):
        """
        Generates a Plotly Figure object representing the stream data.

        Returns:
            A Plotly Figure object.
        """
        return go.Figure(
            data=[
                go.Scatter(
                    x=list(self.dates),
                    y=list(self.views),
                    legendgroup="view",
                    name="View",
                    legendgrouptitle=Legendgrouptitle(text="View"),
                ),
                go.Scatter(
                    x=list(self.dates),
                    y=list(self.carts),
                    name="Cart",
                    legendgroup="cart",
                    legendgrouptitle=Legendgrouptitle(text="Cart"),
                ),
                go.Scatter(
                    x=list(self.dates),
                    y=list(self.purchases),
                    name="Purchase",
                    legendgroup="purchase",
                    legendgrouptitle=Legendgrouptitle(text="Purchase"),
                ),
            ],
            layout=go.Layout(title="Real Time user behavior in the store"),
        )
