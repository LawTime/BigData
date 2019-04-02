import json
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
from jsonmerge import merge
import time
start = time.time()

import sys
from contextlib import contextmanager
from io import StringIO

from subprocess import Popen, PIPE

@contextmanager
def captured_output():
    new_out = StringIO()
    old_out = sys.stdout
    try:
        sys.stdout = new_out
        yield sys.stdout
    finally:
        sys.stdout = old_out

def readerCsv(name):
    name += ".csv"
    fichier = pd.read_csv(name, sep=';', skiprows=[0, 2], header=[0], encoding="utf8")
    return fichier

essais = readerCsv("Essais")

# Retourne le nombre d'étudiants par groupe
def studentsPerGroup():
    studentsPerGroup = essais.groupby('GROUPE')['ÉTUDIANT'].nunique()
    studentsPerGroup = pd.DataFrame(studentsPerGroup, columns=['ÉTUDIANT']).rename(columns={'ÉTUDIANT': 'studentsPerGroup'})
    return studentsPerGroup


# Nombre d'essaie par groupe
def triesPerGroup():
    triesPerGroup = essais.groupby('GROUPE').size()
    triesPerGroup = pd.DataFrame(triesPerGroup)
    triesPerGroup.columns = ['triesPerGroup']
    return triesPerGroup


def mostTriesPerGroup():
    mostTriesPerGroup = triesPerGroup().sort_values(by=['triesPerGroup'], ascending=False)
    mostTriesPerGroup.columns = ['mostTriesPerGroup']
    return mostTriesPerGroup


def averageTriesPerStudent():
    averageTriesPerStudent = {"averageTriesPerStudent": essais.groupby('ÉTUDIANT').size().mean()}
    return averageTriesPerStudent


def averageTriesPerGroupPerStudent():
    averageTriesPerGroupPerStudent = essais.groupby(['GROUPE', 'ÉTUDIANT']).size()
    averageTriesPerGroupPerStudent = pd.DataFrame(averageTriesPerGroupPerStudent, columns=['TESTS']).rename(columns={'TESTS': 'averageTriesPerGroupPerStudent'})
    averageTriesPerGroupPerStudent = averageTriesPerGroupPerStudent.groupby('GROUPE')['averageTriesPerGroupPerStudent'].mean()
    averageTriesPerGroupPerStudent = pd.DataFrame(averageTriesPerGroupPerStudent, columns=['averageTriesPerGroupPerStudent'])
    return averageTriesPerGroupPerStudent


def mostActiveGroup():
    mostActiveGroup = averageTriesPerGroupPerStudent().nlargest(4, 'averageTriesPerGroupPerStudent')
    return mostActiveGroup


def exerciseDonePerGroup():
    exerciseDonePerGroup = essais.groupby('GROUPE')['EXO'].unique()
    exerciseDonePerGroup = pd.DataFrame(exerciseDonePerGroup, columns=['EXO']).rename(columns={'EXO': 'exerciseDonePerGroup'})
    return exerciseDonePerGroup


def exerciseTriedNotSucceeded():
    exerciseTried = essais.EXO.unique()[:-1]
    exerciseSucceeded = essais[(essais['ERREURS'] == 0) & (essais['ECHECS'] == 0)].EXO.unique()
    exerciseTriedNotSucceeded = {"exerciseTriedNotSucceeded": dict((set(exerciseTried) | set(exerciseSucceeded)) - (set(exerciseTried) & set(exerciseSucceeded)))}
    return exerciseTriedNotSucceeded


def tryGroupByExoStudent():
    tryGroupByExoStudent = essais.groupby(['EXO', 'ÉTUDIANT'])
    meanTimeBetweenFirstAndLastTry = pd.DataFrame({"EXO": [], "meanTimeBetweenFirstAndLastTry": []})
    timeByExo = 0
    FMT = '%d/%m/%Y %H:%M:%S'
    oldKey = ''
    count = 0
    for key, item in tryGroupByExoStudent:
        if key[0] == oldKey or oldKey == '':
            for line in item.itertuples():
                if line.ERREURS == 0 and line.ECHECS == 0:
                    count = count + 1
                    timeDelta = datetime.strptime(line.HORODATEUR, FMT) - datetime.strptime(item.iat[0, 0], FMT)
                    timeByExo = timeByExo + timeDelta.total_seconds()
                    oldKey = key[0]
                    break
        else:
            meanTimeBetweenFirstAndLastTry = meanTimeBetweenFirstAndLastTry.append({'EXO': oldKey, 'meanTimeBetweenFirstAndLastTry': timedelta(seconds=(timeByExo / count))}, ignore_index=True)
            timeByExo = 0
            count = 0
            for line in item.itertuples():
                if line.ERREURS == 0 and line.ECHECS == 0:
                    count = count + 1
                    timeDelta = datetime.strptime(line.HORODATEUR, FMT) - datetime.strptime(item.iat[0, 0], FMT)
                    timeByExo = timeByExo + timeDelta.total_seconds()
                    break
            oldKey = ''
    meanTimeBetweenFirstAndLastTry = meanTimeBetweenFirstAndLastTry.append({'EXO': oldKey, 'meanTimeBetweenFirstAndLastTry': timedelta(seconds=(timeByExo / count))}, ignore_index=True).set_index('EXO').sort_values('meanTimeBetweenFirstAndLastTry', ascending=False)
    meanTimeBetweenFirstAndLastTry['meanTimeBetweenFirstAndLastTry'] = meanTimeBetweenFirstAndLastTry['meanTimeBetweenFirstAndLastTry'].astype(str)
    return meanTimeBetweenFirstAndLastTry


def exerciseBestSucceeded():
    exerciseBestSucceeded = essais[(essais['ERREURS'] == 0) & (essais['ECHECS'] == 0)].groupby(['EXO', 'ÉTUDIANT']).nunique()
    exerciseBestSucceeded = pd.DataFrame(exerciseBestSucceeded, columns=['EXO']).rename(columns={'EXO': 'exerciseBestSucceeded'}).sort_values('exerciseBestSucceeded', ascending=0)
    return exerciseBestSucceeded.count(level=0).sort_values('exerciseBestSucceeded', ascending=False)


def studentSucceededOnFirstTry():
    studentSucceededOnFirstTry = essais.groupby(['ÉTUDIANT', 'EXO'])
    count = 0
    oldKey = ''
    for key, item in studentSucceededOnFirstTry:
        if item.iat[0, 5] == 0 and item.iat[0, 6] == 0 and oldKey != key[0]:
            count = count + 1
            oldKey = key[0]

    studentSucceededOnFirstTry = {"studentSucceededOnFirstTry": count}
    return studentSucceededOnFirstTry


def weekdaysMostTries():
    FMT = '%d/%m/%Y %H:%M:%S'
    weekdaysMostTries = pd.DataFrame(pd.to_datetime(essais['HORODATEUR'], format=FMT, errors='coerce').dt.day_name()).groupby('HORODATEUR').size().sort_values(ascending=False).to_frame().rename(columns={0: 'weekdaysMostTries'})
    return weekdaysMostTries


def hoursMostTries():
    FMT = '%d/%m/%Y %H:%M:%S'
    hoursMostTries = pd.Series(pd.to_datetime(essais['HORODATEUR'], format=FMT, errors='coerce')).dt.hour.to_frame().groupby('HORODATEUR').size().sort_values(ascending=False).to_frame().rename(columns={0: 'hoursMostTries'})
    return hoursMostTries


def default_to_regular(d):
    if isinstance(d, defaultdict):
        d = {k: default_to_regular(v) for k, v in d.items()}
    return d

def weekdaysMostTriesByHours():
    FMT = '%d/%m/%Y %H:%M:%S'
    weekdaysMostTriesByHours = pd.DataFrame({"hours": [], "weekday": []})
    weekdaysMostTriesByHours['hours'] = pd.to_datetime(essais['HORODATEUR'], format=FMT, errors='coerce').dt.hour
    weekdaysMostTriesByHours['weekday'] = pd.to_datetime(essais['HORODATEUR'], format=FMT, errors='coerce').dt.day_name()
    weekdaysMostTriesByHours = weekdaysMostTriesByHours.groupby(['weekday', 'hours']).size().to_frame().rename(columns={0: "weekdaysMostTriesByHours"})
    weekdaysMostTriesByHours = weekdaysMostTriesByHours.reset_index().sort_values(['weekday', 'weekdaysMostTriesByHours'], ascending=False).set_index(['weekday', 'hours'])
    results = defaultdict(lambda: defaultdict(dict))
    for index, value in weekdaysMostTriesByHours.itertuples():
        for i, key in enumerate(index):
            if i == 0:
                nested = results[key]
            elif i == len(index) - 1:
                nested[key] = value
            else:
                nested = nested[key]
    results = default_to_regular(results)
    weekdaysMostTriesByHours = {"weekdaysMostTriesByHours": results}
    weekdaysMostTriesByHours = pd.DataFrame.from_dict(weekdaysMostTriesByHours)
    return weekdaysMostTriesByHours


def examsSpikeUsage():
    FMT = '%d/%m/%Y %H:%M:%S'
    examsSpikeUsage = pd.DataFrame({"day": []})
    examsSpikeUsage['day'] = pd.to_datetime(essais['HORODATEUR'], format=FMT, errors='coerce').dt.floor('d')
    examsSpikeUsage = examsSpikeUsage.groupby('day').size().to_frame().rename(columns={0: 'examsSpikeUsage'})
    usageMean = examsSpikeUsage['examsSpikeUsage'].mean()
    if examsSpikeUsage.loc['2018-10-22':'2018-10-26']['examsSpikeUsage'].mean() > usageMean or examsSpikeUsage.loc['2018-12-10':'2018-12-14']['examsSpikeUsage'].mean() > usageMean:
        results = 'Spike Detected'
    else:
        results = 'Spike Not Detected'

    isExamsSpikeUsage = {"examsSpikeUsage": results}
    return isExamsSpikeUsage


def dayMostTries():
    FMT = '%d/%m/%Y %H:%M:%S'
    dayMostTries = pd.DataFrame({"day": []})
    dayMostTries['day'] = pd.to_datetime(essais['HORODATEUR'], format=FMT, errors='coerce').dt.floor('d')
    dayMostTries = dayMostTries.groupby('day').size().to_frame().rename(columns={0: 'dayMostTries'}).sort_values(['dayMostTries'], ascending=False).head(1)
    dayMostTries.index = dayMostTries.index.map(str)
    return dayMostTries


def studentMostTries():
    studentMostTries = essais.groupby(['ÉTUDIANT']).size()
    studentMostTries = pd.DataFrame(studentMostTries, columns=['EXO']).rename(columns={'EXO': 'studentMostTries'}).sort_values('studentMostTries', ascending=0).head(1)
    return studentMostTries


def studentMostSuccess():
    studentMostSuccess = essais[(essais['ERREURS'] == 0) & (essais['ECHECS'] == 0)].groupby(['ÉTUDIANT', 'EXO']).nunique()
    studentMostSuccess = pd.DataFrame(studentMostSuccess, columns=['EXO']).rename(columns={'EXO': 'studentMostSuccess'}).sort_values('studentMostSuccess', ascending=0)
    studentMostSuccess = studentMostSuccess.count(level=0).sort_values('studentMostSuccess', ascending=False).head(1)
    return studentMostSuccess


'''def mostCommonSyntaxError():
    mostCommonSyntaxError = essais
    for index, row in mostCommonSyntaxError.iterrows():
        try:
            def input(optional_arg=''):
                return
            with captured_output() as out:
                subprocess.call(['row.CODE'])
        except Exception as e:
            mostCommonSyntaxError.loc[index, 'ERRORS'] = e
            pass
    mostCommonSyntaxError['ERRORS'] = mostCommonSyntaxError['ERRORS'].astype(str)
    mostCommonSyntaxError = mostCommonSyntaxError[~mostCommonSyntaxError['ERRORS'].isin(['nan'])]
    mostCommonSyntaxError = mostCommonSyntaxError.groupby(['ERRORS']).size().to_frame().dropna().rename(columns={0: 'mostCommonSyntaxError'}).sort_values('mostCommonSyntaxError', ascending=False)
    return mostCommonSyntaxError'''


def json_format(var):
    if type(var) is dict:
        parsed = var
    else:
        parsed = json.loads(var.to_json())
    return parsed


def json_export_file(var):
    with open('Test.json', 'w', encoding='utf-8') as file:
        json.dump(var, file, indent=4, ensure_ascii=False)


result = json_format(studentsPerGroup())
result = merge(result, json_format(triesPerGroup()))
result = merge(result, json_format(mostTriesPerGroup()))
result = merge(result, json_format(averageTriesPerStudent()))
result = merge(result, json_format(averageTriesPerGroupPerStudent()))
result = merge(result, json_format(mostActiveGroup()))
result = merge(result, json_format(exerciseDonePerGroup()))
result = merge(result, json_format(exerciseTriedNotSucceeded()))
result = merge(result, json_format(tryGroupByExoStudent()))
result = merge(result, json_format(exerciseBestSucceeded()))
result = merge(result, json_format(studentSucceededOnFirstTry()))
result = merge(result, json_format(weekdaysMostTries()))
result = merge(result, json_format(hoursMostTries()))
result = merge(result, json_format(weekdaysMostTriesByHours()))
result = merge(result, json_format(examsSpikeUsage()))
result = merge(result, json_format(dayMostTries()))
result = merge(result, json_format(studentMostTries()))
result = merge(result, json_format(studentMostSuccess()))
#result = merge(result, json_format(mostCommonSyntaxError()))

#print(result)

json_export_file(result)

end = time.time()
print(end - start)