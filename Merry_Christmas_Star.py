# importing turtle module
import turtle

# number of sides
n = 32

# creating instance of turtle
pen = turtle.Turtle()

# loop to draw a side
for i in range(n):
    # drawing side of
    # length i*10
    turtle.bgcolor('Black')
    pen.color('Gold')
    pen.forward(i * 10)
    print('Drawing line: ' + str(i))

    # changing direction of pen
    # by 144 degree in clockwise
    pen.right(144)

# closing the instance
pen.color('Black')
pen.hideturtle()

turtle.hideturtle()
turtle.penup()

turtle.sety(-200)

turtle.color('White')

text_string = 'Merry Christmas!'
print(text_string)
turtle.write(text_string, align='center', font=("Arial", 32, "normal"))

turtle.done()
