import React, { useEffect, useRef, useState } from 'react'
import Map from 'ol/Map'
import View from 'ol/View'
import Overlay from 'ol/Overlay'
import TileLayer from 'ol/layer/Tile'
import VectorLayer from 'ol/layer/Vector'
import VectorSource from 'ol/source/Vector'
import OSM from 'ol/source/OSM'
import { fromLonLat } from 'ol/proj'
import Feature from 'ol/Feature'
import { Geometry } from 'ol/geom'
import GeoJSON from 'ol/format/GeoJSON'

// Import minified library side-effect
import '@ngageoint/geopackage/dist/geopackage.min.js'

import 'ol/ol.css'
import './MapComponent.css'

const MapComponent: React.FC = () => {
    const mapRef = useRef<HTMLDivElement>(null)
    const map = useRef<Map | null>(null)
    const popupRef = useRef<HTMLDivElement>(null)
    const overlayRef = useRef<Overlay | null>(null)
    const fileInputRef = useRef<HTMLInputElement>(null)

    const [isLoading, setIsLoading] = useState(false)
    const [loadedLayers, setLoadedLayers] = useState<string[]>([])
    const [error, setError] = useState<string | null>(null)
    const [hoverInfo, setHoverInfo] = useState<Record<string, any> | null>(null)

    useEffect(() => {
        if (!mapRef.current || !popupRef.current) return

        // Initialize Overlay (Popup)
        overlayRef.current = new Overlay({
            element: popupRef.current,
            // FIX: Disable autoPan to stop the map from moving when near edges
            autoPan: false,
            positioning: 'bottom-center',
            offset: [0, -10],
            stopEvent: false,
        })

        // Initialize Map
        map.current = new Map({
            target: mapRef.current,
            layers: [
                new TileLayer({
                    source: new OSM(),
                }),
            ],
            overlays: [overlayRef.current],
            view: new View({
                center: fromLonLat([-123.1207, 49.2827]),
                zoom: 10,
            }),
        })

        // Pointer Move (Hover) Handler
        map.current.on('pointermove', (evt) => {
            if (!map.current || !overlayRef.current) return;

            const pixel = evt.pixel;

            // Check if we are hovering over a feature
            const feature = map.current.forEachFeatureAtPixel(pixel, (feat) => feat);

            // Change cursor style
            map.current.getTargetElement().style.cursor = feature ? 'pointer' : '';

            if (feature) {
                const properties = feature.getProperties();
                const { geometry, ...attributes } = properties;

                setHoverInfo(attributes);
                overlayRef.current.setPosition(evt.coordinate);
            } else {
                setHoverInfo(null);
                overlayRef.current.setPosition(undefined);
            }
        });

        return () => {
            if (map.current) {
                map.current.setTarget(undefined)
            }
        }
    }, [])

    const handleFileUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
        const file = event.target.files?.[0]
        if (!file) return

        if (!file.name.endsWith('.gpkg')) {
            setError('Please select a GeoPackage (.gpkg) file')
            return
        }

        setIsLoading(true)
        setError(null)
        setHoverInfo(null)

        try {
            const arrayBuffer = await file.arrayBuffer()
            const uint8Array = new Uint8Array(arrayBuffer)

            // @ts-ignore
            const GlobalGeoPackage = window.GeoPackage;
            if (!GlobalGeoPackage) throw new Error('GeoPackage library failed to load globally.');

            const GeoPackageAPI = GlobalGeoPackage.GeoPackageAPI || GlobalGeoPackage;
            const setSqljsWasmLocateFile = GlobalGeoPackage.setSqljsWasmLocateFile || GlobalGeoPackage.GeoPackageAPI?.setSqljsWasmLocateFile;

            if (typeof setSqljsWasmLocateFile === 'function') {
                setSqljsWasmLocateFile((filename: string) => '/' + filename);
            }

            const geoPackage = await GeoPackageAPI.open(uint8Array)

            if (!map.current) throw new Error('Map is not initialized')

            const featureTables = geoPackage.getFeatureTables()
            const layerNames: string[] = []
            const geojsonFormat = new GeoJSON()

            for (const tableName of featureTables) {
                try {
                    const iterator = geoPackage.iterateGeoJSONFeatures(tableName);
                    const vectorSource = new VectorSource();
                    const features: Feature<Geometry>[] = [];

                    for (const geoJsonFeature of iterator) {
                        if (geoJsonFeature) {
                            try {
                                const olFeature = geojsonFormat.readFeature(geoJsonFeature, {
                                    dataProjection: 'EPSG:4326',
                                    featureProjection: 'EPSG:3857'
                                })
                                features.push(olFeature)
                            } catch (geomError) {
                                console.warn(`Error converting geometry:`, geomError)
                            }
                        }
                    }

                    if (features.length > 0) {
                        vectorSource.addFeatures(features)

                        const vectorLayer = new VectorLayer({
                            source: vectorSource,
                            style: {
                                'stroke-color': '#ff0000',
                                'stroke-width': 2,
                                'fill-color': 'rgba(255, 0, 0, 0.3)',
                                'circle-radius': 5,
                                'circle-fill-color': '#ff0000',
                            },
                        })

                        map.current?.addLayer(vectorLayer)
                        layerNames.push(tableName)
                    }

                } catch (tableError) {
                    console.warn(`Error loading table ${tableName}:`, tableError)
                }
            }

            if (layerNames.length > 0) {
                const layers = map.current.getLayers().getArray()
                const vectorLayers = layers.filter(layer => layer instanceof VectorLayer) as VectorLayer<VectorSource>[]
                let combinedExtent: number[] | null = null

                vectorLayers.forEach(layer => {
                    const source = layer.getSource()
                    if (source) {
                        const extent = source.getExtent()
                        if (extent && Math.abs(extent[0]) !== Infinity && Math.abs(extent[0]) < 200000000) {
                            if (!combinedExtent) {
                                combinedExtent = extent;
                            } else {
                                combinedExtent = [
                                    Math.min(combinedExtent[0], extent[0]),
                                    Math.min(combinedExtent[1], extent[1]),
                                    Math.max(combinedExtent[2], extent[2]),
                                    Math.max(combinedExtent[3], extent[3])
                                ]
                            }
                        }
                    }
                })

                if (combinedExtent) {
                    map.current.getView().fit(combinedExtent, {
                        padding: [50, 50, 50, 50],
                        maxZoom: 16,
                        duration: 1000
                    })
                }
            }

            setLoadedLayers(layerNames)

        } catch (err) {
            console.error('Error loading GeoPackage:', err)
            setError(`Error loading GeoPackage: ${err instanceof Error ? err.message : 'Unknown error'}`)
        } finally {
            setIsLoading(false)
        }
    }

    const clearLayers = () => {
        if (!map.current) return
        const layers = map.current.getLayers().getArray()
        for (let i = layers.length - 1; i > 0; i--) {
            map.current.removeLayer(layers[i])
        }
        setLoadedLayers([])
        setError(null)
        setHoverInfo(null)
        overlayRef.current?.setPosition(undefined)
        if (fileInputRef.current) fileInputRef.current.value = ''
    }

    return (
        <div className="map-container">
            <div className="map-controls">
                <div className="file-input-container">
                    <input
                        ref={fileInputRef}
                        type="file"
                        accept=".gpkg"
                        onChange={handleFileUpload}
                        disabled={isLoading}
                        className="file-input"
                        id="geopackage-file"
                    />
                    <label htmlFor="geopackage-file" className="file-input-label">
                        {isLoading ? 'Loading...' : 'Choose GeoPackage File'}
                    </label>
                    {loadedLayers.length > 0 && (
                        <button onClick={clearLayers} className="clear-button">
                            Clear Layers
                        </button>
                    )}
                </div>

                {error && <div className="error-message">{error}</div>}

                {loadedLayers.length > 0 && (
                    <div className="loaded-layers">
                        <h3>Loaded Layers:</h3>
                        <ul>
                            {loadedLayers.map((layerName, index) => <li key={index}>{layerName}</li>)}
                        </ul>
                    </div>
                )}
            </div>

            <div ref={mapRef} className="map" />

            <div ref={popupRef} className="ol-popup">
                {hoverInfo && (
                    <div className="popup-content">
                        <h4>Feature Info</h4>
                        {Object.entries(hoverInfo).map(([key, value]) => (
                            <div key={key} className="popup-row">
                                <strong>{key}:</strong> <span>{String(value)}</span>
                            </div>
                        ))}
                    </div>
                )}
            </div>
        </div>
    )
}

export default MapComponent