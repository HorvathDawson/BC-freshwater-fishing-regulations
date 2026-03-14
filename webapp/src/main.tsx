import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'
// Data service preloads — disabled while transitioning to FreshWaterAtlas PMTiles.
// TODO: Re-enable when regulation data is available via reaches.json
// import { regulationsService } from './services/regulationsService'
// import { waterbodyDataService } from './services/waterbodyDataService'
// waterbodyDataService.preload();
// regulationsService.preload();

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
