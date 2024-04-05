import pandas as pd
import numpy as np


class ChartGenerator:
    def __init__(self, filePath: str, matchId: str) -> None:
        self.filePath: str = filePath
        self.df: pd.DataFrame = pd.read_csv(filePath)
        self.reportData: dict = {}
        self.matchId = matchId


    def startReportGeneration(self) -> None:
        self.report_1()
        self.report_2()
        self.report_3()
        self.report_4()
        self.report_5()
        self.report_6()
        self.report_7()
        self.report_8()
        self.report_9()
        self.report_10()


    def getReportData(self) -> dict:
        data: dict = {
            "metadata": {
                "type": "REPORT",
                "match_id": self.matchId,
                "report_types": {
                    "report_1": "BAR",
                    "report_2": "BAR",
                    "report_3": "PIE",
                    "report_4": "PIE",
                    "report_6": "PIE",
                    "report_7": "HIST",
                    "report_8": "BAR",
                    "report_10": "PIE",
                }
            },
            "data": self.reportData
        }

        return data


    def report_1(self) -> None:
        dfWeapon: pd.DataFrame = self.df.copy()

        weapon_dmg_sum: pd.Series = dfWeapon.groupby("weaponUsed")["dmgDone"].sum()

        weapon_dmg_sum = weapon_dmg_sum.sort_values(ascending=False)
        data_int = weapon_dmg_sum.values.astype(int)
        labels = [label.replace('weapon_', '') for label in weapon_dmg_sum.index]

        self.reportData["report_1"] = {
            "labels" : labels,
            "data" : data_int.tolist()
        }


    def report_2(self) -> None:
        df_copy: pd.DataFrame = self.df.copy()

        hit_area_mapping = {
            0.0: 'GENERIC',
            1.0: 'HEAD',
            2.0: 'CHEST',
            3.0: 'STOMACH',
            4.0: 'LEFTARM',
            5.0: 'RIGHTARM',
            6.0: 'LEFTLEG',
            7.0: 'RIGHTLEG',
            8.0: 'GEAR',
            'nan': 'NaN'
        }

        df_copy['targetHitArea'] = df_copy['targetHitArea'].map(hit_area_mapping)
        hit_area_counts = df_copy['targetHitArea'].value_counts()

        self.reportData["report_2"] = {
            "labels" : hit_area_counts.index.tolist(),
            "data" : hit_area_counts.values.tolist()
        }


    def report_3(self) -> None:
        dfCat: pd.DataFrame = self.df.copy()

        category_dmg_sum = dfCat.groupby("weaponCategory")["dmgDone"].sum()
        category_dmg_sum = category_dmg_sum.sort_values(ascending=False)
        data_int = category_dmg_sum.values.astype(int)

        labels = [label.replace('weapon_category_', '') for label in category_dmg_sum.index]

        self.reportData["report_3"] = {
            "labels" : labels,
            "data" : data_int.tolist()
        }


    def report_4(self) -> None:
        df_copy: pd.DataFrame = self.df.copy()

        def analyze_movement_types(dataframe: pd.DataFrame):
            filtered_data: pd.DataFrame =  dataframe[dataframe['isHurt'] == False]

            crouching_count = int(filtered_data['isCrouching'].sum()) if 'isCrouching' in filtered_data else 0
            jumping_count = int(filtered_data['isInAir'].sum()) if 'isInAir' in filtered_data else 0
            no_movement_count = len(filtered_data) - (crouching_count + jumping_count)

            return crouching_count, jumping_count, no_movement_count

        crouching_count, jumping_count, no_movement_count = analyze_movement_types(df_copy)

        labels = []
        sizes = []

        if crouching_count > 0:
            labels.append('Crouching')
            sizes.append(crouching_count)

        if jumping_count > 0:
            labels.append('Jumping')
            sizes.append(jumping_count)

        if no_movement_count > 0:
            labels.append('No Movement')
            sizes.append(no_movement_count)

        labels, sizes = zip(*sorted(zip(labels, sizes), key=lambda x: x[1], reverse=True))

        self.reportData["report_4"] = {
            "labels" : list(labels),
            "data" : list(sizes)
        }
        

    def report_5(self) -> None:        
        dfQ5: pd.DataFrame = self.df.copy()

        distance_columns = ['distToTarget', 'weaponCategory', 'weaponUsed']
        filtered_data = dfQ5[distance_columns].dropna()

        distance_bins = [0, 50, 200, 400, 600, 800, 1000, float('inf')]
        distance_labels = ['0-50', '50-200', '200-400', '400-600', '600-800', '800-1000', '1000+']

        filtered_data['DistanceCategory'] = pd.cut(filtered_data['distToTarget'], bins=distance_bins, labels=distance_labels, right=False)
        grouped_data = filtered_data.groupby(['weaponCategory', 'weaponUsed', 'DistanceCategory'], observed= False).size().reset_index(name='Count')
        grouped_data = grouped_data.sort_values(['weaponCategory', 'weaponUsed', 'DistanceCategory'])

        unique_weapon_categories = grouped_data['weaponCategory'].unique()
        
        # preparing the empty report object
        self.reportData["report_5"] = dict()
        for category in unique_weapon_categories:
            self.reportData["report_5"][category] = dict()

        for category in unique_weapon_categories:
            category_data = grouped_data[grouped_data['weaponCategory'] == category]
            unique_weapons_in_category = category_data['weaponUsed'].unique()

            for weapon in unique_weapons_in_category:
                weapon_data: pd.DataFrame = category_data[category_data['weaponUsed'] == weapon]

                if weapon_data['Count'].sum() != 0:
                    # Calculate precomputed values
                    labels = weapon_data['DistanceCategory'].tolist()
                    values = weapon_data['Count'].tolist()

                    # Print precomputed values
                    self.reportData["report_5"][category][weapon] = {
                        "labels" : labels,
                        "data" : values
                    }


    def report_6(self) -> None:
        df_copy: pd.DataFrame = self.df[self.df["isHurt"] == False].copy()

        def count_obstructed_shots(dataframe: pd.DataFrame):
            total_shots = len(dataframe)

            obstructed_shots = int(((df_copy['isFlashed'] > 0) | (df_copy['shotTargetThroughSmoke'].astype(bool))).sum())

            return total_shots, obstructed_shots
        
        total_shots, obstructed_shots = count_obstructed_shots(df_copy)

        labels = ['Obstructed Shots', 'Non-Obstructed Shots']
        sizes = [obstructed_shots, total_shots - obstructed_shots]

        self.reportData["report_6"] = {
            "labels" : str(labels),
            "data" : sizes
        }
        

    def report_7(self) -> None:
        df_copy: pd.DataFrame = self.df.copy()

        data = df_copy["pitch"]
        df_histo = pd.DataFrame(data)

        def compute_histogram_data(df_histo: pd.DataFrame, column_name: str, bin_width=1):
            min_value = -90  # min value for pitch
            max_value = 90  # max value for pitch
            bins = np.arange(min_value, max_value + bin_width, bin_width)
            hist, edges = np.histogram(df_histo[column_name], bins=bins)
            return hist, edges[:-1]

        hist, edges = compute_histogram_data(df_histo, 'pitch')

        self.reportData["report_7"] = {
            "labels" : hist.tolist(),
            "data" : edges.tolist()
        }


    def report_8(self) -> None:
        df_copy: pd.DataFrame = self.df.copy()
        utility_columns = ['utilityDmgDone', 'supportUtilityUsed']

        filtered_data = df_copy[utility_columns].dropna()
        average_utility_dmg_done = float(filtered_data['utilityDmgDone'].mean())
        average_support_utility_used = float(filtered_data['supportUtilityUsed'].mean())

        labels = ['Average Utility Damage Done', 'Average Support Utility Used']
        values = [average_utility_dmg_done, average_support_utility_used]

        self.reportData["report_8"] = {
            "labels" : labels,
            "data" : values
        }
    

    def report_9(self) -> None:
        dfSnipe: pd.DataFrame = self.df.copy()

        sniper_data = dfSnipe[dfSnipe['weaponCategory'] == 'weapon_category_sniper']
        scoping_counts = sniper_data['isScoping'].value_counts()

        labels = ['Scoping', 'Not Scoping']
        values = [int(scoping_counts.get(True, 0)), int(scoping_counts.get(False, 0))]

        self.reportData["report_9"] = {
            "weapon_sniper" : dict(),
            "weapon_ar" : dict()
        }

        self.reportData["report_9"]["weapon_sniper"] = {
            "labels" : labels,
            "data" : values
        }

        # for Aug and SG556
        dfScope: pd.DataFrame = self.df.copy()

        ar_data = dfScope[(dfScope['weaponCategory'] == 'weapon_category_ar') & ((dfScope['weaponUsed'] == 'weapon_aug') | (dfScope['weaponUsed'] == 'weapon_sg556'))]
        scoping_counts = ar_data['isScoping'].value_counts()

        labels = ['Scoping', 'Not Scoping']
        values = [int(scoping_counts.get(True, 0)), int(scoping_counts.get(False, 0))]

        self.reportData["report_9"]["weapon_ar"] = {
            "labels" : labels,
            "data" : values
        }


    def report_10(self) -> None:
        df_copy: pd.DataFrame = self.df.copy()

        def count_blind_shots(df_copy: pd.DataFrame):
            blind_data = df_copy[df_copy['isTargetBlind'].fillna(False).astype(bool)]
            not_blind_data = df_copy[df_copy['isTargetBlind'].fillna(True).astype(bool)]

            blind_shot_count = len(blind_data)
            not_blind_shot_count = len(not_blind_data)

            return blind_shot_count, not_blind_shot_count

        blind_shot_count, not_blind_shot_count = count_blind_shots(df_copy)
        labels = ['Blind Shots', 'Not Blind Shots']
        sizes = [blind_shot_count, not_blind_shot_count]

        self.reportData["report_10"] = {
            "labels" : labels,
            "data" : sizes
        }