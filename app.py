from flask import Flask, request, jsonify
from mvp_comp_dir.mvp_comp_compute import AllCompetitions, workflow_compute_mvp
import traceback

app = Flask(__name__)


@app.route('/')
def home():
    return jsonify({"message": "MVP SPI Flask API is live!"})


@app.route('/competitions', methods=['GET'])
def get_all_competitions():
    try:
        all_comps = AllCompetitions()
        all_comp_info = all_comps.gather_all_competition_ids("https://www.fotmob.com/")
        return jsonify({"status": "success", "competitions": all_comp_info})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e), "trace": traceback.format_exc()}), 500




@app.route('/add_competition', methods=['POST'])
def add_competition():
    data = request.json
    try:
        competition_name = data.get("competition_name", "")
        defined_url = data.get("defined_url", "")  # optional

        all_comps = AllCompetitions()
        all_comp_info = all_comps.gather_all_competition_ids("https://www.fotmob.com/")

        result = all_comps.add_competition_to_my_watchlist(
            competition_name=competition_name,
            gather_all_competition_ids=all_comp_info,
            defined_url=defined_url
        )
        return jsonify({"status": "success", "message": result})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e), "trace": traceback.format_exc()}), 500




#     def compute_mvp(self, competition_name: str, competition_year: str, scalar: int=4, open_close_league:str="", title:str="", percentile_threshold:float=.98, manual_competition_id:str="")->None:


@app.route('/run_analysis', methods=['POST'])
def run_analysis():
    data = request.json
    try:
        competition_name = str(data["competition_name"])
        competition_year = str(data["competition_year"])
        scalar = int(data.get("scalar", 4))
        open_close_league = data.get("open_close_league", "")
        overide = bool(data.get("overide", True))
        title = data.get("title", "")
        percentile_threshold = float(data.get("percentile_threshold", 0.98))
        manual_competition_id = data.get("manual_competition_id", "")
        
        # print(competition_name, competition_year)

        result_path = workflow_compute_mvp(
            competition_name=competition_name,
            competition_year=competition_year,
            scalar=scalar,
            open_close_league=open_close_league,
            overide=overide,
            title=title,
            percentile_threshold=percentile_threshold,
            manual_competition_id=manual_competition_id
        )

        return jsonify({"status": "success", "output_path": result_path})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e), "trace": traceback.format_exc()}), 500


if __name__ == '__main__':
    app.run(debug=True)