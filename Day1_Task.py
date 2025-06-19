# how to print
# "" is string which we have to print 
print("Hello world!")

## \n is for next new line
print("1. Mix 500g of Flour, 10g Yeast and 300ml Water in a bowl")
print("2. Knead the dough for 10 minutes\n")

## concateation
print("Hello" + " " "Ashesh")
# + is used for sting concateation i.e, for adding  & " " is for giving space 
print("Hello" " Ashesh")
# we can use space after sting "Hello " or  before sring " Ashesh"
# [Note:-check spaces or tab while printing because it give indentation error]

 ##input function
input("What is your Name?")
print("Hello " + input ("What is your Name?") + "!")

#for comment at linux ctrl + /   and for windows ctrl + /
# but if want to make normal repeat same process
#for eg ;
input("What is your Name?")
print("Hello " + input ("What is your Name?") + "!")

##python variable which is use to store value
name = input("What is your name? \n")
print(name)
OR 
name ="Ashesh"
print(name)

#find length of name use (len function)
name = input("What is your name? \n")
print(len(name))
OR 
print(len(input("What is your Name? \n")))

#To store seperate variable
username = input("What is your Name? \n")
length =len(username)
print(length)

##variable Naming
n= "Ashesh"
l= len(n)
print(l)
OR
name= "Ashesh"
length= len(name)
print(length)

##final task
print("Welcome to the Band Name Generator \n")
city = input("Which city did you grow up in? \n")
pet = input("What is the name of your pet? \n")
print("Your city:" +city+ " \n" "Your pet:" + pet)