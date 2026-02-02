from typing import List

def encode(plain_text: str, dictionary) -> List[int]:
    dict_list = []
    # Trasformo il dizionario da stringa a lista di caratteri
    if type(dictionary) is str:
        dict_list = []
        for i in dictionary:
            dict_list.append(i)
    else:
        dict_list = dictionary

    # Transformation
    compressed_text = list()          # Regular array
    rank = 0

    # Read in each character
    for c in plain_text:
        rank = dict_list.index(c)    # Find the rank of the character in the dictionary [O(k)]
        compressed_text.append(rank)  # Update the encoded text

        # Update the dictionary [O(k)]
        dict_list.pop(rank)
        dict_list.insert(0, c)

    return compressed_text            # Return the encoded text

def decode(compressed_text: List[int], dictionary) -> str:
    
    dict_list = []
    # Trasformo il dizionario da stringa a lista di caratteri
    if type(dictionary) is str:
        dict_list = []
        for i in dictionary:
            dict_list.append(i)
    else:
        dict_list = dictionary

    plain_text = []

    # Read in each rank in the encoded text
    for rank in compressed_text:
        # Read the character of that rank from the dictionary
        plain_text.append(dict_list[rank])

        # Update the dictionary
        e = dict_list.pop(rank)
        dict_list.insert(0, e)

    return "".join(plain_text)  # Return original string


if __name__ == "__main__":
    input = "bananaaa"
    dictionary = "abcdefghijklmnopqrstuvwxyz"
    list = encode(input, dictionary)
    print("CODIFICA: " + str(list))
    dec = decode(list, dictionary)
    print("DECODIFICA:" + str(dec))