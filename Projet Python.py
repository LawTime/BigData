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

def reader_csv(name):
    name += ".csv"
    fichier = pd.read_csv(name, sep=';', skiprows=[0, 2], header=[0], encoding="utf8")
    return fichier

essais = reader_csv("Essais")

# Retourne le nombre d'étudiants par groupe
def studentsPerGroup():
    studentsPerGroup = essais.groupby('GROUPE')['ÉTUDIANT'].nunique()
    studentsPerGroup = pd.DataFrame(studentsPerGroup, columns=['ÉTUDIANT']).rename(columns={'ÉTUDIANT': 'studentsPerGroup'})
    return studentsPerGroup


# Nombre d'essaie par groupe
def tries_per_group():
    tries_per_group = essais.groupby('GROUPE').size()
    tries_per_group = pd.DataFrame(tries_per_group)
    tries_per_group.columns = ['tries_per_group']
    return tries_per_group


def most_tries_per_group():
    most_tries_per_group = tries_per_group().sort_values(by=['tries_per_group'], ascending=False)
    most_tries_per_group.columns = ['most_tries_per_group']
    return most_tries_per_group


def average_tries_per_student():
    average_tries_per_student = {"average_tries_per_student": essais.groupby('ÉTUDIANT').size().mean()}
    return average_tries_per_student


def average_tries_per_group_per_student():
    average_tries_per_group_per_student = essais.groupby(['GROUPE', 'ÉTUDIANT']).size()
    average_tries_per_group_per_student = pd.DataFrame(average_tries_per_group_per_student, columns=['TESTS']).rename(columns={'TESTS': 'average_tries_per_group_per_student'})
    average_tries_per_group_per_student = average_tries_per_group_per_student.groupby('GROUPE')['average_tries_per_group_per_student'].mean()
    average_tries_per_group_per_student = pd.DataFrame(average_tries_per_group_per_student, columns=['average_tries_per_group_per_student'])
    return average_tries_per_group_per_student


def most_active_group():
    most_active_group = average_tries_per_group_per_student().nlargest(4, 'average_tries_per_group_per_student')
    return most_active_group


def exercise_done_per_group():
    exercise_done_per_group = essais.groupby('GROUPE')['EXO'].unique()
    exercise_done_per_group = pd.DataFrame(exercise_done_per_group, columns=['EXO']).rename(columns={'EXO': 'exercise_done_per_group'})
    return exercise_done_per_group


def exercise_tried_not_succeeded():
    exerciseTried = essais.EXO.unique()[:-1]
    exerciseSucceeded = essais[(essais['ERREURS'] == 0) & (essais['ECHECS'] == 0)].EXO.unique()
    exercise_tried_not_succeeded = {"exercise_tried_not_succeeded": dict((set(exerciseTried) | set(exerciseSucceeded)) - (set(exerciseTried) & set(exerciseSucceeded)))}
    return exercise_tried_not_succeeded


def iter_item(item, count, time_by_exo, old_key, key, FMT):
    for line in item.itertuples():
        if line.ERREURS == 0 and line.ECHECS == 0:
            count = count + 1
            time_delta = datetime.strptime(line.HORODATEUR, FMT) - datetime.strptime(item.iat[0, 0], FMT)
            time_by_exo = time_by_exo + time_delta.total_seconds()
            old_key = key[0]
            break
    return item, count, time_by_exo, old_key, key, FMT

def try_group_by_exo_student():
    try_group_by_exo_student = essais.groupby(['EXO', 'ÉTUDIANT'])
    mean_time_between_first_and_last_try = pd.DataFrame({"EXO": [], "mean_time_between_first_and_last_try": []})
    time_by_exo = 0
    FMT = '%d/%m/%Y %H:%M:%S'
    old_key = ''
    count = 0
    for key, item in try_group_by_exo_student:
        if key[0] == old_key or old_key == '':
            item, count, time_by_exo, old_key, key, FMT = iter_item(item, count, time_by_exo, old_key, key, FMT)
        else:
            mean_time_between_first_and_last_try = mean_time_between_first_and_last_try.append({'EXO': old_key, 'mean_time_between_first_and_last_try': timedelta(seconds=(time_by_exo / count))}, ignore_index=True)
            time_by_exo = 0
            count = 0
            item, count, time_by_exo, old_key, key, FMT = iter_item(item, count, time_by_exo, old_key, key, FMT)
            old_key = ''
    mean_time_between_first_and_last_try = mean_time_between_first_and_last_try.append({'EXO': old_key, 'mean_time_between_first_and_last_try': timedelta(seconds=(time_by_exo / count))}, ignore_index=True).set_index('EXO').sort_values('mean_time_between_first_and_last_try', ascending=False)
    mean_time_between_first_and_last_try['mean_time_between_first_and_last_try'] = mean_time_between_first_and_last_try['mean_time_between_first_and_last_try'].astype(str)
    return mean_time_between_first_and_last_try


def exercise_best_succeeded():
    exercise_best_succeeded = essais[(essais['ERREURS'] == 0) & (essais['ECHECS'] == 0)].groupby(['EXO', 'ÉTUDIANT']).nunique()
    exercise_best_succeeded = pd.DataFrame(exercise_best_succeeded, columns=['EXO']).rename(columns={'EXO': 'exercise_best_succeeded'}).sort_values('exercise_best_succeeded', ascending=0)
    return exercise_best_succeeded.count(level=0).sort_values('exercise_best_succeeded', ascending=False)


def student_succeeded_on_first_try():
    student_succeeded_on_first_try = essais.groupby(['ÉTUDIANT', 'EXO'])
    count = 0
    old_key = ''
    for key, item in student_succeeded_on_first_try:
        if item.iat[0, 5] == 0 and item.iat[0, 6] == 0 and old_key != key[0]:
            count = count + 1
            old_key = key[0]

    student_succeeded_on_first_try = {"student_succeeded_on_first_try": count}
    return student_succeeded_on_first_try


def weekdays_most_tries():
    FMT = '%d/%m/%Y %H:%M:%S'
    weekdays_most_tries = pd.DataFrame(pd.to_datetime(essais['HORODATEUR'], format=FMT, errors='coerce').dt.day_name()).groupby('HORODATEUR').size().sort_values(ascending=False).to_frame().rename(columns={0: 'weekdays_most_tries'})
    return weekdays_most_tries


def hours_most_tries():
    FMT = '%d/%m/%Y %H:%M:%S'
    hours_most_tries = pd.Series(pd.to_datetime(essais['HORODATEUR'], format=FMT, errors='coerce')).dt.hour.to_frame().groupby('HORODATEUR').size().sort_values(ascending=False).to_frame().rename(columns={0: 'hours_most_tries'})
    return hours_most_tries


def default_to_regular(d):
    if isinstance(d, defaultdict):
        d = {k: default_to_regular(v) for k, v in d.items()}
    return d

def weekdays_most_tries_by_hours():
    FMT = '%d/%m/%Y %H:%M:%S'
    weekdays_most_tries_by_hours = pd.DataFrame({"hours": [], "weekday": []})
    weekdays_most_tries_by_hours['hours'] = pd.to_datetime(essais['HORODATEUR'], format=FMT, errors='coerce').dt.hour
    weekdays_most_tries_by_hours['weekday'] = pd.to_datetime(essais['HORODATEUR'], format=FMT, errors='coerce').dt.day_name()
    weekdays_most_tries_by_hours = weekdays_most_tries_by_hours.groupby(['weekday', 'hours']).size().to_frame().rename(columns={0: "weekdays_most_tries_by_hours"})
    weekdays_most_tries_by_hours = weekdays_most_tries_by_hours.reset_index().sort_values(['weekday', 'weekdays_most_tries_by_hours'], ascending=False).set_index(['weekday', 'hours'])
    results = defaultdict(lambda: defaultdict(dict))
    for index, value in weekdays_most_tries_by_hours.itertuples():
        for i, key in enumerate(index):
            if i == 0:
                nested = results[key]
            elif i == len(index) - 1:
                nested[key] = value
            else:
                nested = nested[key]
    results = default_to_regular(results)
    weekdays_most_tries_by_hours = {"weekdays_most_tries_by_hours": results}
    weekdays_most_tries_by_hours = pd.DataFrame.from_dict(weekdays_most_tries_by_hours)
    return weekdays_most_tries_by_hours


def exams_spike_usage():
    FMT = '%d/%m/%Y %H:%M:%S'
    exams_spike_usage = pd.DataFrame({"day": []})
    exams_spike_usage['day'] = pd.to_datetime(essais['HORODATEUR'], format=FMT, errors='coerce').dt.floor('d')
    exams_spike_usage = exams_spike_usage.groupby('day').size().to_frame().rename(columns={0: 'exams_spike_usage'})
    usage_mean = exams_spike_usage['exams_spike_usage'].mean()
    if exams_spike_usage.loc['2018-10-22':'2018-10-26']['exams_spike_usage'].mean() > usage_mean or exams_spike_usage.loc['2018-12-10':'2018-12-14']['exams_spike_usage'].mean() > usage_mean:
        results = 'Spike Detected'
    else:
        results = 'Spike Not Detected'

    is_exams_spike_usage = {"exams_spike_usage": results}
    return is_exams_spike_usage


def day_most_tries():
    FMT = '%d/%m/%Y %H:%M:%S'
    day_most_tries = pd.DataFrame({"day": []})
    day_most_tries['day'] = pd.to_datetime(essais['HORODATEUR'], format=FMT, errors='coerce').dt.floor('d')
    day_most_tries = day_most_tries.groupby('day').size().to_frame().rename(columns={0: 'day_most_tries'}).sort_values(['day_most_tries'], ascending=False).head(1)
    day_most_tries.index = day_most_tries.index.map(str)
    return day_most_tries


def student_most_tries():
    student_most_tries = essais.groupby(['ÉTUDIANT']).size()
    student_most_tries = pd.DataFrame(student_most_tries, columns=['EXO']).rename(columns={'EXO': 'student_most_tries'}).sort_values('student_most_tries', ascending=0).head(1)
    return student_most_tries


def student_most_success():
    student_most_success = essais[(essais['ERREURS'] == 0) & (essais['ECHECS'] == 0)].groupby(['ÉTUDIANT', 'EXO']).nunique()
    student_most_success = pd.DataFrame(student_most_success, columns=['EXO']).rename(columns={'EXO': 'student_most_success'}).sort_values('student_most_success', ascending=0)
    student_most_success = student_most_success.count(level=0).sort_values('student_most_success', ascending=False).head(1)
    return student_most_success


'''def most_common_syntax_error():
    most_common_syntax_error = essais
    for index, row in most_common_syntax_error.iterrows():
        try:
            def input(optional_arg=''):
                return
            with captured_output() as out:
                subprocess.call(['row.CODE'])
        except Exception as e:
            most_common_syntax_error.loc[index, 'ERRORS'] = e
            pass
    most_common_syntax_error['ERRORS'] = most_common_syntax_error['ERRORS'].astype(str)
    most_common_syntax_error = most_common_syntax_error[~most_common_syntax_error['ERRORS'].isin(['nan'])]
    most_common_syntax_error = most_common_syntax_error.groupby(['ERRORS']).size().to_frame().dropna().rename(columns={0: 'most_common_syntax_error'}).sort_values('most_common_syntax_error', ascending=False)
    return most_common_syntax_error'''


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
result = merge(result, json_format(tries_per_group()))
result = merge(result, json_format(most_tries_per_group()))
result = merge(result, json_format(average_tries_per_student()))
result = merge(result, json_format(average_tries_per_group_per_student()))
result = merge(result, json_format(most_active_group()))
result = merge(result, json_format(exercise_done_per_group()))
result = merge(result, json_format(exercise_tried_not_succeeded()))
result = merge(result, json_format(try_group_by_exo_student()))
result = merge(result, json_format(exercise_best_succeeded()))
result = merge(result, json_format(student_succeeded_on_first_try()))
result = merge(result, json_format(weekdays_most_tries()))
result = merge(result, json_format(hours_most_tries()))
result = merge(result, json_format(weekdays_most_tries_by_hours()))
result = merge(result, json_format(exams_spike_usage()))
result = merge(result, json_format(day_most_tries()))
result = merge(result, json_format(student_most_tries()))
result = merge(result, json_format(student_most_success()))
#result = merge(result, json_format(most_common_syntax_error()))

#print(result)

json_export_file(result)

end = time.time()
print(end - start)