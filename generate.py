import sys
import random
import itertools
from itertools import chain

def main(argv):
    cmdFile = open("cmds.txt", "w")

    # Add P nodes without replacement, P = 4, 8, 10, 20, 30, and others
    # Message
    population = [4, 8, 10, 20, 30]
    sampleNodes = range(1, 256)
    sampleNodes.remove(4)
    sampleNodes.remove(8)
    sampleNodes.remove(10)
    sampleNodes.remove(20)
    sampleNodes.remove(30)

    for i in population:
        cmdFile.write("join " + str(i) + "\n")
        cmdFile.write("wait 0.5\n")

    for i in range(1, 11):
        random.shuffle(sampleNodes)
        curNode = sampleNodes[0:1]
        population.append(curNode[0])

        cmdFile.write("join " + str(curNode[0]) + "\n")
        cmdFile.write("wait 0.5\n")

    cmdFile.write("wait 2\n")
    cmdFile.write("message\n")
    cmdFile.write("wait 0.5\n")
    
    # Find >= 64 times, with p & k randomly selected... p from the P set
    # Message / Find times

    for i in range(1, 100):
        findNode = random.sample(population, 1)
        cmdFile.write("find " + str(findNode[0]) + " " + str(random.randint(0, 255)) + "\n")
        cmdFile.write("wait 0.5\n")

    cmdFile.write("wait 2\n")
    cmdFile.write("message\n")
    cmdFile.write("wait 0.5\n")


if __name__ == '__main__':
    main(sys.argv)