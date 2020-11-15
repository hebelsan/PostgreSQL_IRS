arr = [[1,2,3], [4,5], [7,6], [10,8]]


def min_dist(arr):
    min_dist_sum = 0
    min_dist = -1

    # TODO !!
    if len(arr) < 2:
        print(0)

    for k in range(0, len(arr) - 1):
        for i in range(1, len(arr) - k):
            min_dist = -1
            for valAr2 in arr[k+i]:
                for valAr1 in arr[k]:
                    print(str(valAr1) + " " + str(valAr2))
                    if min_dist == -1:
                        min_dist = abs(valAr1-valAr2)
                    else:
                        if min_dist > abs(valAr1-valAr2):
                            min_dist = abs(valAr1-valAr2)
            min_dist_sum += 1. / (min_dist**2)
    
    print(min_dist_sum)


if __name__ == "__main__":
    min_dist(arr)
