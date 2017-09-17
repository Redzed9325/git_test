Liste = [3,4,5,9,"JD"]
def fonction (Liste):
    for x in Liste:
        if type(x) != int:
            Liste.remove(x)
    return Liste
    print ("somme = " + sum(Liste))
    print ("moyenne = " + (sum(Liste)/x))
