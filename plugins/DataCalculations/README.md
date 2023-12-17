
# Strategy Creation Guide

This guide illustrates the process of creating a new strategy utilizing the `BaseMetrics` class and integrating it into the existing framework.

## Getting Started

### Creating a New Strategy

1. **File Location:** Begin by creating a new Python file within `DataCalculations/strategies/base_metrics_abc.py`. This file will house your new strategy classes.
   
   **Example:**
   ```python
   # DataCalculations/strategies/base_metrics_abc.py

   from abc import ABC
   import logging

   # ... (Existing code)
   ```

2. **Inheritance:** Inherit from the `BaseMetrics` class for your new strategy. This allows leveraging the base functionalities.

   **Example:**
   ```python
   class MyCustomMetric(BaseMetrics):
       columns = ['column1', 'column2', 'column3']

       def __init__(self, dataframe):
           super().__init__(dataframe)
           self._validate()
   ```

3. **Define Columns:** Specify the columns required for your calculations. Define the `columns` attribute within your strategy class.

   **Example:**
   ```python
   class MyCustomMetric(BaseMetrics):
       columns = ['column1', 'column2', 'column3']

       # ... (Other methods)
   ```

4. **Implement Calculations:** Develop specific calculation methods within your strategy class prefixed with `calculate_`.

   **Example:**
   ```python
   class MyCustomMetric(BaseMetrics):
       columns = ['column1', 'column2', 'column3']

       def calculate_correct_predictions(self):
           ''' Perform calculations here '''
           your_result = 0  # Replace this with your calculation logic
           return your_result
   ```

5. **Integration:** Integrate your new strategy into the existing framework by adding it to the `calculators` list within the `PerformanceCalculator` class.

   **Example:**
   ```python
   class PerformanceCalculator:
       calculators = [
           # Existing strategies
           MyCustomMetric,  # Add your new strategy here
       ]

       # ... (Other code)
   ```

6. **Testing:** Develop comprehensive tests in `DataCalculations/tests/test_strategy.py` to validate the functionality of your new strategy.

## Template Example

Here's a template example to assist in creating a new strategy:

```python
class MyCustomMetric(BaseMetrics):
    columns = ['column1', 'column2', 'column3']

    def __init__(self, dataframe):
        super().__init__(dataframe)
        self._validate()

    def calculate_correct_predictions(self):
        ''' Perform calculations here '''
        your_result = 0  # Replace this with your calculation logic
        return your_result
```
