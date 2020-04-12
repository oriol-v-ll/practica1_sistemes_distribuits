#
# Pràctica 1 sistemes distribuïts.
# Oriol Villanova Llorens
#


#Importem les llibreries necessaries
import pywren_ibm_cloud as pywren
import numpy as np
import pickle
import ibm_boto3
from time import time


#Inicialització de matrius i guardat al IBM COS
# workers = tamany de la matriu; ibm_cos per poder fer funcions del ibm_cloud
def inicialitzacio(workers,ibm_cos):

    
    #Sistema sequencial amb workers=1; es pujen les matrius senceres al COS 
    if workers == 1:
        #Es creen dos matrius d'exemple. 
        #A = np.array([[1,2,3],[4,5,6],[7,8,9]])
        #B = np.array([[1,2,3],[4,5,6],[7,8,9]])

        A = np.random.random((10,10))  
        B = np.random.random((10,10))
        
        
        ibm_cos.put_object(Bucket ='oriol1', Key='matriuA.txt', Body= A.dumps())
        ibm_cos.put_object(Bucket = 'oriol1', Key= 'matriuB.txt', Body= B.dumps())

    elif ((workers == 3) or (workers == 9)):
        
        #Es creen dos matrius d'exemple. 
        A = np.array([[1,2,3],[4,5,6],[7,8,9]])
        B = np.array([[1,2,3],[4,5,6],[7,8,9]])

        #Es crea una llista per a guardar el nom dels fitxers
        
        matriu = []
        
        # Dividim les matrius en files i columnes i les pujem al COS
    
        for i in range (0, 3): # Canivar el valor de 3 per el tamany de la matriu.
            matAfili = A[i,:]
            ibm_cos.put_object(Bucket='oriol1', Key='fila'+str(i)+'matriuA.txt', Body=matAfili.dumps())
            matriu.append('fila'+str(i)+'matriuA.txt')
        for j in range (0, 3):
            matBfilj = B[:,j]
            ibm_cos.put_object(Bucket='oriol1', Key='columna'+str(j)+'matriuB.txt', Body=matBfilj.dumps())
            matriu.append('columna'+str(j)+'matriuB.txt')

        #Retornarem la llista amb els noms dels fitxers.
        return matriu  

    elif ((workers != 1) and (workers != 3) and (workers != 9)):
        #hem de fer la matriu del tamany dels workers que es volen utilitzar  
        #
        A = np.random.random((workers,workers))  
        B = np.random.random((workers,workers))
  
        matriu = []

        for i in range (workers): 
            matAfili = A[i,:]
            ibm_cos.put_object(Bucket='oriol1', Key='fila'+str(i)+'matriuA.txt', Body=matAfili.dumps())
            matriu.append('fila'+str(i)+'matriuA.txt')
        for j in range (workers):
            matBfilj = B[:,j]
            ibm_cos.put_object(Bucket='oriol1', Key='columna'+str(j)+'matriuB.txt', Body=matBfilj.dumps())
            matriu.append('columna'+str(j)+'matriuB.txt')





#Es multipliquen les parts de les matrius que calen
def multiplicacio(fila, columna, workers, ibm_cos):

    if (workers == 9):

        #Es baixen els objectes del object storage depenent del numero de workers. 
        A = ibm_cos.get_object(Bucket = 'oriol1', Key = 'fila'+str(fila)+'matriuA.txt') ['Body'].read() 
        B = ibm_cos.get_object(Bucket = 'oriol1', Key = 'columna'+str(columna)+'matriuB.txt') ['Body'].read()

        #Es deserialitzen la fila i la columna que volem multiplicar
      
        filaA = pickle.loads(A)
        columnaB = pickle.loads(B)

        #fem la multiplicació i pujem el resultat al COS.
        C = filaA.dot(columnaB)

        ibm_cos.put_object(Bucket='oriol1', Key='C'+str(fila)+str(columna)+'.txt', Body=C.dumps())

        return C
          
    elif (workers == 1):
        #Es descarrega els objectes corresponents del IBM COS
        A = ibm_cos.get_object(Bucket = 'oriol1', Key = 'matriuA.txt')['Body'].read()
        B = ibm_cos.get_object(Bucket = 'oriol1', Key = 'matriuB.txt')['Body'].read()
    
        #Es deserialitzen els objectes que venen del object storage.
        matriuA = pickle.loads(A)
        matriuB = pickle.loads(B)
    
        #Es fa la multiplicació de matrius. 
        C = matriuA.dot(matriuB)
        #Es retorna al call_async la matriu ja multiplicada.
        return C

    else:
        A = ibm_cos.get_object(Bucket = 'oriol1', Key = 'fila'+str(fila)+'matriuA.txt') ['Body'].read() 
        filaA = pickle.loads(A)
        M = []

        for i in range(0,workers):
            M.append([])
            #Hem descarrego els fitxers necesaris per treballar amb 3 workers
            CB = ibm_cos.get_object(Bucket = 'oriol1', Key = 'columna'+str(i)+'matriuB.txt') ['Body'].read()
            #Deserialitzo els fitxers que necessito en cada cas
            columnaB = pickle.loads(CB)
             #Faig la multiplicació que necessita cada worker
            C = filaA.dot(columnaB)
            M[i] = C 
            #fico al cos els resultats. 
            #ibm_cos.put_object(Bucket='oriol1', Key='C'+str(fila)+str(i)+'.txt', Body=C.dumps())
    return M
    


    
    

#Es junta la matriu final.
def juntar (results, ibm_cos):

    
    total = []
    i = 0
    for map_result in results:
        total.append([])
        total[i] = map_result
        i = i + 1 

    return total


   
if __name__ == '__main__':

    print("Model sequencial (w = 1) o model paralel (w = 9 o w = 3)/ Sortir = 4 || Si vols treballar amb diferents valors de la matriu = 5")
    workers = int(input())

    while (workers != 4):
        #Es comprova que el numero de workers es el correcte
        if (workers < 101): 
            if (workers > 0):
                # workers = 1 -> funcio sequencial
                if (workers == 1):
                    #Comprovo el temps que ha trigat en funcionar la matriu:
                    start_time = time()    
                    #es fa la crida sequencial
                    pw = pywren.ibm_cf_executor()
                    pw.call_async(inicialitzacio, workers)
                    pw.call_async(multiplicacio, (1,1,workers))
                    #Miro el temps que ha trigat
                    elapsed_time = time() - start_time
                    #es necessita retornar un paràmetre de les funcions per a poder escriurel
                    print(pw.get_result())
                    print('El temps que ha trigat sequencialment es: ' + str(elapsed_time) + 'segons')
                # workes == 9 es crida el map.   
                elif (workers == 9 or workers == 3):
                    #Inicio un contador per a saber el temps que triga la operació
                    start_time = time()
                    #executo la matriu que volem aconseguir
                    pw = pywren.ibm_cf_executor()
                    matriu = np.empty(workers)
                    pw.call_async(inicialitzacio, workers)
                    fitxers = pw.get_result()
                    operacions = 9 #operacions necesaries per una matriu de 3x3. 
                    #Creo la matriu iterdata. --> Com que treballo amb matrius de 3x3 treballaré amb 3 workers o en 9. 
                    cada_worker = operacions / workers 
                    if (workers == 3): #es comprova per a quina cantitat de workers es treballa 
                            
                        #es genera el vector iterdata amb el que sigui necessari. 
                        iterdata = [(0,0,workers), (1,0,workers), (2,0,workers)]

                    elif (workers == 9): #es comprova per a quina cantitat de workers es treballa 
                            
                        #es genera el vector iterdata amb el que sigui necessari. 
                        iterdata = [(0,0,workers), (0,1,workers), (0,2,workers), (1, 0,workers), (1, 1,workers), (1, 2,workers), (2, 0,workers), (2, 1,workers), (2, 2,workers)]

                    

                    futures = pw.map_reduce(multiplicacio, iterdata,juntar)
                    #paro el contador de temps
                    pw.wait(futures)
                    elapsed_time = time() - start_time
                    print(pw.get_result())
                    print('El temps que ha trigat paralelament es: ' + str(elapsed_time) + 'segons')

                elif (workers == 5):
                    #Hi haurà tants treballadors com files tingui la matriu.

                    print("Matrius cuadrades quin valor vols = (3x3..4x4...)")
                    tamany = int(input())
                    
                    #Inicio un contador per a saber el temps que triga la operació
                    start_time = time()
                    #iniciem la trucada asíncrona.
                    pw = pywren.ibm_cf_executor()
                    matriu = np.empty(workers)
                    pw.call_async(inicialitzacio, tamany)
                    fitxers = pw.get_result()

                    #depenent del tamany farem un valor diferent de iterdata. 
                    iterdata = []
                    fila = 0
                    for i in range (tamany):
                        #afegim valors a iterdata tants com siguin necesaris.
                        iterdata.append([])
                        iterdata[i] = (i, 0, tamany)
                    
                    # un cop fet l'iterdata pasem al mode paralel amb els workers que volem treballar
                    workers =  tamany 
                    futures = pw.map_reduce(multiplicacio, iterdata,juntar)
                    pw.wait(futures)
                    elapsed_time = time() - start_time
                    print(pw.get_result())
                    print('El temps que ha trigat paralelament es: ' + str(elapsed_time) + 'segons')





        else:
            print("S'ha d'escollir entre el model sequencial(W=1) o el paralel(W=9)")

        print("Model sequencial (w = 1) o model paralel (w = 9 o w = 3)/ Sortir = 4 || Si vols treballar amb diferents valors de la matriu = 5")
        workers = int(input())



