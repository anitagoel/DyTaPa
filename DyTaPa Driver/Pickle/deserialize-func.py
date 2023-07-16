import os
import json
import pickle
import marshal, types

# This is a function to de serialize a function in a file
#def findSum(num_list):
#    result = sum(num_list)
#    return result

def main():
    numlist = list(range(1, 10))
    print("Contents of the list: ", numlist)
    sumFile = "sumFunc.txt"
    # Load and execute the function
    try:
        with open(sumFile, 'rb') as f:
            pFunc = f.read()
            # q = pickle.loads(pFunc)
            code = marshal.loads(pFunc)
            func = types.FunctionType(code, globals(), "some_func_name")
            res = func(numlist)
            print("Sum of the values in the list", res)
    except IOError:
        print("Error: could not create file " + filename)


if __name__ == "__main__":
    main()