# -*- coding: utf-8 -*-
"""sumoffirstnfib.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1MzWeJbVBbys7DKy6L01f68u-7y0v0fGv
"""

#Code to get the sum of first N fibnacci numbers
sum = 1
prev_1 = 0
prev_2 = 0
n= 25


if n<10 or n>100:
	print("Input must be between 10 and 100")
temp =1
for i in range(1,n):
	prev_2 = prev_1
	prev_1 = temp
	sum +=  prev_1+prev_2
	temp = prev_1+prev_2 

print("Given 'n': ",n)
print("Sum of first n fibonacci numbers: ", sum)