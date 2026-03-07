import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'
import { regulationsService } from './services/regulationsService'
import { waterbodyDataService } from './services/waterbodyDataService'

// Preload data on app startup — waterbody JSON is the largest payload
// and sits on the critical path; kick it off as early as possible.
waterbodyDataService.preload();
regulationsService.preload();

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
