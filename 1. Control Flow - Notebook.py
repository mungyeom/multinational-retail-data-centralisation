# -*- coding: utf-8 -*-
"""Notebook.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/github/AI-Core/Content-Public/blob/main/Content/units/Essentials/7.%20Python%20programming/13.%20Control%20Flow/Notebook.ipynb

# Control Flow

## Learning Objectives
- Understand the concept of control flow in basic programming.
- Learn how to use 'if' statements.
- Learn how to use 'elif' and 'else' statements.

# Introduction

- Control flow refers to the order of execution of instructions in a computer program.
- A program is a series of instructions called statements.
- The order in which these statements are executed can vary.
- Control flow facilitates decision making (conditional statements), branching and looping in code.

## Conditional Statements

Conditional statements are a form of control flow. They comprise a set of instructions in code that are executed if a certain condition is met.

### If statements

<p align=center><img src=https://github.com/AI-Core/Content-Public/blob/main/Content/units/Essentials/7.%20Python%20programming/13.%20Control%20Flow/images/if.svg?raw=1 width=400></p>

- An if statement is a piece of code that causes another piece of code to be executed based on the fulfillment of a condition.
- If statements employ boolean operators to determine if a condition is true and whether or not to execute the dependent piece of code subsequently.
- The basic syntax for an if statement in pseudo-code is as follows:
"""

# if some_condition:
#     do_something

"""- Note the indentation. Python determines precedence based on indentation/whitespace rather than brackets, as in other languages.
- The standard practice for indentation, as recommended by the PEP8 guidelines, is to use four spaces; however, you can also use a tab.
- Spaces and tabs should not be mixed for indentation.
- Jupyter Notebook, similar to many other IDEs, automatically creates an indentation after a colon.

### Elif and else statements

<p align=center><img src=https://github.com/AI-Core/Content-Public/blob/main/Content/units/Essentials/7.%20Python%20programming/13.%20Control%20Flow/images/if_else.svg?raw=1 width=400></p>

- Elif ('else if') statements add a block of code to be executed if the first condition is not fulfilled, i.e. if the first 'if' statement does not run.
- Else statements add a block of code to be executed if none of the previous conditions are fulfilled, i.e. if the 'if' and 'elif' statements do not run.
- The syntax is as follows (take note of the whitespace alignment):
"""

# if some_condition:
#     do_something
# elif some_other_condition:
#     do_something_else
# else:
#     do_this

"""### Examples"""

x = input('Enter your age')
x = int(x) # Input will return a string, you need an integer
if x >= 21:
    print("You are allowed to drink in the US")
elif x >= 18:
    print("You are allowed to drink, but not in the US")
elif x < 0:
    print('Wait, what??')
else:
    print('You can have a Fanta')

a = int(input('Enter a number for A'))
b = int(input('Enter a number for B'))

print(f'A is equal to {a}')
print(f'B  is equal to {b}')
if a > b:
    print("A is larger than B")
elif a < b:
    print ("A is smaller than B")
else:
    print ("A is equal to B")

"""The `if-else` statements can also be run in one line:"""

if a > b: print("a is greater than b")

print("A") if a > b else print("B")

"""Any condition type can be applied in an `if` statement, provided that it returns a single boolean."""

a = int(input('Enter a number for A'))
b = int(input('Enter a number for B'))
c = int(input('Enter a number for C'))

if a > b and b > c:
  print("Both conditions are True")

"""## Conclusion
At this point, you should have a good understanding of

- the basics of control flow and the order of code execution.
- how to employ conditional statements to manipulate control flow in code.


## Further Reading
- Control flow: https://docs.python.org/3.8/tutorial/controlflow.html?highlight=control%20flow
"""