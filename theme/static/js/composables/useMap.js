import * as L from 'https://unpkg.com/leaflet@1.7.1/dist/leaflet-src.esm.js'
import { watch, onMounted } from 'https://cdnjs.cloudflare.com/ajax/libs/vue/3.0.7/vue.esm-browser.prod.js'

export default function useMap(mapElemId, geoJsonData, itemsPath, tileLayerUrl, tileLayerAttr) {
  let map, layerItems

  // map initialize
  const setupMap = function() {
    map = L.map(mapElemId).setView([45, -75], 5)
    map.addLayer(new L.TileLayer(
        tileLayerUrl, {
          maxZoom: 18,
          attribution: tileLayerAttr
        }
    ))
    layerItems = new L.GeoJSON({type: 'FeatureCollection', features: []})
    map.addLayer(layerItems)
  }
  onMounted(setupMap)

  // update map with new geoJson data
  watch(geoJsonData, () => {
    if (map.hasLayer(layerItems)) {
      map.removeLayer(layerItems)
    }
    
    if (Object.prototype.hasOwnProperty.call(geoJsonData.value, 'features')) {
      if (geoJsonData.value.features[0].geometry === null) {
        map.setView([0, 0], 1)
      } else {
        layerItems = new L.GeoJSON(geoJsonData.value, {
          onEachFeature: function (feature, layer) {
            let url = itemsPath + '/' + feature.id + '?f=html'
            let html = '<span><a href="' + url + '">' + feature.id + '</a></span>'
            layer.bindPopup(html)
          }
        })
        map.addLayer(layerItems)
        if (geoJsonData.value.features.length !== 0) {
          map.fitBounds(layerItems.getBounds(), {maxZoom: 5})
        } else {
          map.setView([45, -75], 1)
        }
      }
    } else {
      map.setView([45, -75], 1)
    }
  })

  return {
    setupMap
  }
}