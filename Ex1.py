##print("Hello\n there?",)
# name=input("what is your name?")
# print("name is "+ name )


# number=input("Enter a number")

# print("the number is of type" , type(number))

# if number.isdigit():
#     print("Its a digit")
# elif number.replace('.','',1).isdigit():
#     print("its a float")
# elif number in ["TRUE","FALSE"]:
#     print("Its boolean")
# else:
#     print("Its string")

import random

score=0

for i in range(1,6):
    num1 = random.randint(1,10)
    num2 = random.randint(1,10)
    op=random.choice(['+','-','*','/'])

    question = f"{num1} {op} {num2}"
    correct_ans = eval(question)

    print("question is", question)
    user_ans = input('enetr the ans')

    try:
        if float(correct_ans) == float(user_ans):
            print("Correct")
            score+=1
        else:
            print("wrong")
    
    except ValueError:
        print("enter a valid number")

print("scrore is",score)



