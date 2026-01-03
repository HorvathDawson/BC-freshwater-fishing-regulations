import './App.css'
import MapComponent from './components/MapComponent'

function App() {
  return (
    <div className="App">
      <header className="app-header">
        <h1>GeoPackage Map Viewer</h1>
        <p>Load and view GeoPackage files on an interactive map</p>
      </header>
      <main className="app-main">
        <MapComponent />
      </main>
    </div>
  )
}

export default App
