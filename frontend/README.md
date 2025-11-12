# Economic Data Frontend

A React frontend application with FastAPI backend for visualizing market data from MotherDuck.

## Project Structure

```
frontend/
├── backend/          # FastAPI backend
├── frontend/          # React frontend
└── README.md          # This file
```

## Prerequisites

- Python 3.10 or higher
- Node.js 18 or higher
- npm or yarn
- MotherDuck account and token
- Access to the economic data database

## Setup

### Backend Setup

1. Navigate to the backend directory:
```bash
cd frontend/backend
```

2. Create a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file in the `backend/` directory:
```env
MOTHERDUCK_TOKEN=your_motherduck_token_here
MOTHERDUCK_DATABASE=your_database_name
MOTHERDUCK_SCHEMA=public
API_KEY=your_api_key_here
HOST=0.0.0.0
PORT=8000
```

5. Start the backend server:
```bash
python -m app.main
# Or using uvicorn directly:
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

The API will be available at `http://localhost:8000` and API documentation at `http://localhost:8000/docs`.

### Frontend Setup

1. Navigate to the frontend directory:
```bash
cd frontend/frontend
```

2. Install dependencies:
```bash
npm install
# or
yarn install
```

3. Create a `.env` file in the `frontend/` directory (optional):
```env
VITE_API_URL=http://localhost:8000
VITE_API_KEY=your_api_key_here
```

4. Start the development server:
```bash
npm run dev
# or
yarn dev
```

The frontend will be available at `http://localhost:3000`.

## Usage

1. Start the backend server first (see Backend Setup)
2. Start the frontend development server (see Frontend Setup)
3. Open your browser to `http://localhost:3000`
4. Use the dashboard controls to:
   - Select a market data category (currency, global_markets, major_indicies, us_sector)
   - Filter by symbol and time period
   - Switch between summary and time-series views
   - Choose different chart types (returns, risk-return, comparison)

## API Endpoints

All endpoints require the `X-API-Key` header for authentication.

- `GET /api/market-data/categories` - List available categories
- `GET /api/market-data/summary/{category}` - Get summary data
- `GET /api/market-data/analysis/{category}` - Get time-series analysis data
- `GET /api/market-data/symbols/{category}` - List symbols for a category

See the interactive API documentation at `http://localhost:8000/docs` for detailed endpoint information.

## Development

### Backend Development

The backend uses FastAPI with automatic reloading. Changes to Python files will automatically restart the server.

### Frontend Development

The frontend uses Vite with hot module replacement. Changes to React components will automatically update in the browser.

## Building for Production

### Backend

The backend can be deployed using any ASGI server (uvicorn, gunicorn, etc.):

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### Frontend

Build the frontend for production:

```bash
cd frontend
npm run build
```

The built files will be in the `dist/` directory and can be served by any static file server.

## Troubleshooting

### Backend Issues

- **Connection Error**: Verify your `MOTHERDUCK_TOKEN` is correct and you have access to the database
- **Table Not Found**: Ensure the dbt models have been run and tables exist in MotherDuck
- **API Key Error**: Check that the `API_KEY` in your `.env` matches the key used in the frontend

### Frontend Issues

- **API Connection Error**: Verify the backend is running and `VITE_API_URL` is correct
- **CORS Errors**: The backend is configured to allow all origins in development. For production, update CORS settings in `backend/app/main.py`
- **Chart Not Rendering**: Check browser console for errors and ensure Plotly.js is properly loaded

## Technologies Used

- **Backend**: FastAPI, DuckDB, Pydantic
- **Frontend**: React, TypeScript, Vite, Plotly.js, Axios

