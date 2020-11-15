arr = [[1, 6, 9], [2, 4], [11]]


def generatePermutations(lists, result, depth, current):
    if depth == (len(lists)):
        result.append(current)
        return
    
    for i in range(0, len(lists[depth])):
        newList = list(current)
        newList.append(lists[depth][i])
        generatePermutations(lists, result, depth + 1,  newList )

def min_span(arr):
    min_span = -1
    permus = []
    generatePermutations(arr, permus, 0, [])
    print(permus)
    for arr in permus:
        if min_span == -1:
            min_span = max(arr) - min(arr)
        elif min_span > (max(arr) - min(arr)):
            min_span = max(arr) - min(arr)
    return 1.0/(float(min_span)/len(arr))**2

if __name__ == "__main__":
    print(min_span(arr))
