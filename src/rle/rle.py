import re
class Rle:

	def rle_merge(self, rle_blocks):
		output = ""
		#Controllo i blocchi RLE per unire eventuali caratteri uguali
		for i in range(len(rle_blocks)-1):
		
			#Controllo se l'ultimo elemento del blocco i è uguale al primo del blocco i+1
			# block è [value] oppure [count-value]
			last_block_i = rle_blocks[i][-1] 
			first_block_i1 = rle_blocks[i+1][0]

			#se last block è [count-value]
			if "-" in last_block_i:
				count_i, char_i = last_block_i.split("-") #separo count e char
				count_i = int(count_i)
			else:
				#se last block è [value] allora count = 1
				count_i = 1
				char_i = last_block_i

			#se first block è [count-value]
			if "-" in first_block_i1:
				count_i1, char_i1 = first_block_i1.split("-") #separo count e char
				count_i1 = int(count_i1)
			else:
				#se first block è [value] allora count = 1
				count_i1 = 1 
				char_i1 = first_block_i1

			#controllo se i due caratteri sono uguali
			if char_i == char_i1:
				#Unisco i due blocchi somando i contatori
				total_count = count_i + count_i1
				#creo il nuovo elemento come [count-value] sommando i due contatori
				new_element = str(total_count) + "-" + str(char_i)
		
				#Aggiorno i due blocchi
				#Rimuovo l'ultimo elemento del blocco i e lo sostituisco con il nuovo elemento con contatore aggiornato
				rle_blocks[i] = ",".join(rle_blocks[i].split(",")[:-1] + [new_element])
				
				#Rimuovo il primo elemento del blocco i+1
				rle_blocks[i+1] = ",".join(rle_blocks[i+1].split(",")[1:])

		output = "".join(rle_blocks) 
		return output[:-1] #Rimuovo l'ultima virgola

	def rle_encode(self, data):
		encoding = ''
		prev_char = ''
		count = 1

		if not data: return ''
		# print("JJJJJ" + str(data))
		# data = data.split(",")
		for char in data:
			# If the prev and current characters
			# don't match...
			if char != prev_char:
				# ...then add the count and character
				# to our encoding
				if prev_char:
					if count >= 2:
						encoding += str(count) + "-" + str(prev_char) + ","
					else:
						encoding += str(prev_char) + ","
				count = 1
				prev_char = char
			else:
				# Or increment our counter
				# if the characters do match
				count += 1
		else:
			# Finish off the encoding
			if count >= 2:
				encoding += str(count) +"-" + str(prev_char) +","
			else:
				encoding += str(prev_char) + ","
			return encoding[:-1]


	def parallel_rle_encode(self, data):
		# Protezione input vuoto
		if data is None or len(data) == 0: 
			return ''
		
		output_list = []
		
		# Prendiamo il primo elemento subito per inizializzare
		# Questo ci permette di accettare 'data' anche come numpy array di interi
		# senza doverli convertire prima in stringhe (enorme risparmio di tempo)
		prev_char = data[0]
		count = 1
		
		# Itera dal secondo elemento fino alla fine
		for i in range(1, len(data)):
			char = data[i]
			if char != prev_char:
				# Aggiunge al buffer (molto più veloce di += su stringhe lunghe)
				if count >= 2:
					output_list.append(f"{count}-{prev_char}")
				else:
					output_list.append(str(prev_char))
				
				count = 1
				prev_char = char
			else:
				count += 1
		
		# Aggiungi l'ultimo blocco rimasto appeso (la parte "else" del ciclo originale)
		if count >= 2:
			output_list.append(f"{count}-{prev_char}")
		else:
			output_list.append(str(prev_char))
			
		# IMPORTANTE: Il codice originale terminava la stringa con una virgola ",".
		# join crea "a,b,c", quindi aggiungiamo manualmete la "," finale
		# per non rompere il codice che segue (es. il merge).
		return ",".join(output_list) + ","


	def rle_decode(self, data):
		decode = ''
		data = data.split(",")
		for char in data:
			# If the character is numerical...
			if char != "":
				x = re.search("-\d+$", char)
				if x:
					char = char.split("-")
					decode += (char[1]+",") * int(char[0])
				else:
					decode += char + ","
		#delete last element of the string
		return decode[:-1]
	
	def parallel_rle_decode(self, data):
		decode = ''
		data = data.split(",")
		for char in data:
			# If the character is numerical...
			if char != "":
				x = re.search("-\d+$", char)
				if x:
					char = char.split("-")
					decode += (char[1]+",") * int(char[0])
				else:
					decode += char + ","
		
		return decode

if __name__ == "__main__":
	rle = Rle()
	val = "58,22,39,11,0,21,21,21,21,54,25,48,10,13,57,60,3,39,64,0,0,41,68,21,14,3,31,14,33,29,7,66,0,0,69,2,37,0,0,0,60,0,42,43,38,0,42,6,42,1,28,13,51,31,45,70,1,1,57,46,0,16,13,3"
	encoded_val = rle.rle_encode(val)
	print(encoded_val + " lenght ->" + str(len(encoded_val)))
	decodedVal = rle.rle_decode(encoded_val)
	print(decodedVal + " lenght ->" + str(len(decodedVal)))
	if decodedVal == val:
		print("tranf ok")
