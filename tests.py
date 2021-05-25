import szpark
import pandas as pd


def test_calculate_laps():
    laps = pd.read_csv('test_csv/lap_times.csv')
    result = szpark.calculate_laps(laps)
    d = {
        'raceId': [841],
        'driverId': [20],
        'avglaptime': [93794.6]
    }
    expected = pd.DataFrame(data=d)
    assert result.equals(expected), 'calculate laps returns incorrect data'
    print('test_calculate_laps OK...')


def test_calculate_driver():
    drivers = pd.read_csv('test_csv/drivers.csv')
    results = pd.read_csv('test_csv/results.csv')
    races = pd.read_csv('test_csv/races.csv')
    result = szpark.calculate_driver(results, races, drivers)
    d = {
        'raceId': [18, 18, 18],
        'driverId': [1, 2, 3],
        'driverage': [23, 30, 22],
        'numberofraces': [1, 1, 1]
    }
    expected = pd.DataFrame(data=d)
    assert result.equals(expected), 'calculate driver returns incorrect data'
    print('test_calculate_driver OK...')


def test_calculate_pitstops():
    pit_stops = pd.read_csv('test_csv/pit_stops.csv')
    result = szpark.calculate_pit_stops(pit_stops)

    d = {
        'raceId': [1052, 1052],
        'driverId': [852, 854],
        'pitstopnumber': [2, 2],
        'avgpitstopduration': [24687.0, 25570.5]
    }
    expected = pd.DataFrame(data=d)
    assert result.equals(expected), 'calculate pitstops returns incorrect data'
    print('calculate_pitstops OK...')


if __name__ == "__main__":
    test_calculate_laps()
    test_calculate_driver()
    test_calculate_pitstops()
    print("Everything passed")
