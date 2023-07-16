import os
import json
import pickle
import marshal, types

#This is a function to serialize a function in a file

def findMax(num_list):
    result = max(num_list)
    return result

def findMin(num_list):
    result = min(num_list)
    return result

def findSum(num_list):
    result = sum(num_list)
    return result

maxFile = "maxFunc.txt"
try:
    with open(maxFile, 'wb') as f:
        codeString = marshal.dumps(findMax.__code__)
        f.write(codeString)
except IOError:
    print("Error: could not create file " + filename)

minFile = "minFunc.txt"
try:
    with open(minFile, 'wb') as f:
        codeString = marshal.dumps(findMin.__code__)
        f.write(codeString)
except IOError:
    print("Error: could not create file " + filename)

sumFile = "sumFunc.txt"
try:
    with open(sumFile, 'wb') as f:
        codeString = marshal.dumps(findSum.__code__)
        f.write(codeString)
except IOError:
    print("Error: could not create file " + filename)

# Load and execute the function
numlist = list(range(1, 10))
print("Contents of the list: ", numlist)
try:
    with open(minFile, 'rb') as f:
        pFunc = f.read()
        #q = pickle.loads(pFunc)
        code = marshal.loads(pFunc)
        func = types.FunctionType(code, globals(), "some_func_name")
        res = func(numlist)
        print("Sum of the values in the list", res )
except IOError:
    print("Error: could not create file " + filename)

