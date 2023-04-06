#!./mon/bin/python3
import json
import os
import sys

storage = "./jsons"
ns_path = "./jsons/nslist.json"
d_path = './jsons/domainslist.json'

def helper():
    print('\nПомощь в использовании:')
    print('> Управление DNS серверами:')
    print('\t Отобразить список NS на контроль:')
    print('\t control.py ns -l')
    print('\t Добавить (изменить IP) NS на контроль:')
    print('\t control.py ns -a <FQDN>,<IP>,<NS_GROUP>')
    print('\t Удаить NS из контроля:')
    print('\t control.py ns -r <FQDN>\n')
    print('> Управление доменами:')
    print('\t Отобразить список доменов на контроль:')
    print('\t control.py d -l')
    print('\t Добавить (изменить) домен на контроль:')
    print('\t control.py d -a <FQDN>,<NS_GROUP>')
    print('\t Удаить домен из контроля:')
    print('\t control.py d -r <FQDN>\n')

def list_ns():
    if os.path.exists(ns_path):
        with open(ns_path, "r") as f:
            nslist = json.loads(f.read())
            print("Список NS на контроле:")
            for key in nslist:
                print(f"{key}:\t{nslist[key]}")
    else:
        print('Список NS - пуст')

def list_d():
    if os.path.exists(d_path):
        with open(d_path, "r") as f:
            dlist = json.loads(f.read())
            print("Список доменов на контроле:")
            for key in dlist:
                print(key)
    else:
        print('Список доменов - пуст')

def insert_ns(fqdn, ip, group):
    if os.path.exists(ns_path):
        with open(ns_path, "r") as f:
            nslist = json.loads(f.read())
            nslist[fqdn] = [ip, group]
    else: nslist = {fqdn: ip}
    with open(ns_path, "w+") as f:
        #print(json.dumps(nslist, indent=4))
        json.dump(nslist, f, indent=4)

def insert_d(fqdn, group):
    if os.path.exists(d_path):
        with open(d_path, "r") as f:
            dlist = json.loads(f.read())
            try:
                dlist[group].append(fqdn)
            except:
                dlist[group]=[fqdn]
    else: dlist = {fqdn: ns}
    with open(d_path, "w+") as f:
        #print(json.dumps(nslist, indent=4))
        json.dump(dlist, f, indent=4)

def remove_ns(fqdn):
    if os.path.exists(ns_path):
        with open(ns_path, "r") as f:
            newnslist= {}
            nslist = json.loads(f.read())
            for key in nslist:
                if not key == fqdn:
                    newnslist[key] = nslist[key]
        with open(ns_path, "w+") as f:
            #print(json.dumps(newnslist, indent=4))
            json.dump(newnslist, f, indent=4)
    else:
        print('Список NS - пуст')

def remove_d(fqdn):
    if os.path.exists(d_path):
        with open(d_path, "r") as f:
            dlist = json.loads(f.read())
            for key in dlist:
                newdlist= []
                for ns in dlist[key]:
                    if not ns == fqdn:
                        newdlist.append(ns)
                dlist[key] = newdlist
        with open(d_path, "w+") as f:
            #print(json.dumps(newnslist, indent=4))
            json.dump(dlist, f, indent=4)
    else:
        print('Список NS - пуст')                         

if __name__ == "__main__":
    #Проверка наличия папки с данными
    if not os.path.exists(storage):
        os.makedirs(storage)
    flags = sys.argv[1::]
    #Режим работы с DNS серверами
    if flags and flags[0] == 'ns':
        for arg in flags[1::]:
            #Список доменов:
            if arg == '-l':
                i = flags.index(arg) + 1
                list_ns()
                sys.exit()
            #Добавление/Изменение сервера на контроль
            elif arg == '-a':
                i = flags.index(arg) + 1
                try:
                    ns = flags[i].split(',')
                    insert_ns(ns[0], ns[1], ns[2])
                except:
                    print('Неккоретный ввод, пример:')
                    print('control.py ns -a tinirog.ru,1.1.1.1, vtb')
                sys.exit()
            #Удаление сервера из контроля
            elif arg == '-r':
                i = flags.index(arg) + 1
                try:
                    ns = flags[i].split(',')
                    remove_ns(ns[0])
                except:
                    print('Неккоретный ввод, пример:')
                    print('control.py ns -r tinirog.ru')
                sys.exit()
            #Не правильный флаг
            else:
                print(f'{arg} - Не корректный флаг')
                helper()
                sys.exit()
    #Режим работы с доменами
    if flags and flags[0] == 'd':
        for arg in flags[1::]:
            #Список доменов:
            if arg == '-l':
                i = flags.index(arg) + 1
                list_d()
                sys.exit()
            #Добавление домена на контроль
            elif arg == '-a':
                i = flags.index(arg)+1
                try:
                    d = flags[i].split(',')
                    insert_d(d[0], d[1])
                except:
                    print('Неккоретный ввод, пример:')
                    print('control.py ns -d www.tinirog.ru,tinirog')
                sys.exit()
            #Удаление домена из контроля
            elif arg == '-r':
                i = flags.index(arg) + 1
                #try:
                d = flags[i].split(',')
                remove_d(d[0])
                #except:
                #    print('Неккоретный ввод, пример:')
                #    print('control.py ns -d www.tinirog.ru')
                sys.exit()
            else:
                print(f'{arg} - Не корректный флаг')
                helper()
                sys.exit()
    else:
        helper()
