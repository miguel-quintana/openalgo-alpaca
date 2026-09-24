import os
from datetime import datetime
from flask import jsonify, make_response, request
from flask_restx import Namespace, Resource
from database.equity_history_db import get_equity_history
from utils.logging import get_logger

logger = get_logger(__name__)
api = Namespace("equity", description="Account Equity History API")

@api.route("/history", strict_slashes=False)
class EquityHistory(Resource):
    def post(self):
        """Get historical portfolio equity curve"""
        try:
            payload = request.json or {}
            
            # Parse incoming ISO strings if provided
            start_date = payload.get("start_date")
            if start_date:
                start_date = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
                
            end_date = payload.get("end_date")
            if end_date:
                end_date = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
            
            records = get_equity_history(start_date, end_date)
            
            data = []
            for record in records:
                data.append({
                    "timestamp": record.timestamp.isoformat(),
                    "cash": record.cash,
                    "portfolio_value": record.portfolio_value
                })
                
            return make_response(jsonify({"status": "success", "data": data}), 200)
            
        except Exception as e:
            logger.exception(f"Error fetching equity history: {e}")
            return make_response(jsonify({"status": "error", "message": str(e)}), 500)