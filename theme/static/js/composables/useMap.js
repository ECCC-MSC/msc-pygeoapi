import * as L from 'https://unpkg.com/leaflet@1.7.1/dist/leaflet-src.esm.js'
import { ref, computed, watch, onMounted } from 'https://cdnjs.cloudflare.com/ajax/libs/vue/3.0.7/vue.esm-browser.prod.js'

export default function useMap(mapElemId, geoJsonData, itemsPath, tileLayerUrl, tileLayerAttr, bboxPermalink, locale) {
  let map, layerItems
  const maxZoom = 15
  const bbox = ref('')
  // Leaflet LatLngBounds simple array format
  const bboxLatLngBounds = computed(() => {
    const bboxSplit = bbox.value.split(',')
    if (bboxSplit.length < 4) {
      return [[-85, -175], [85, 175]]
    } else {
      return [
        [bboxSplit[1], bboxSplit[0]], 
        [bboxSplit[3], bboxSplit[2]]
      ]
    }
  })
  const updateBboxEvent = function(evt) {
    const bboxString = evt.sourceTarget.getBounds().toBBoxString()
    const bboxSplit = bboxString.split(',').map((x, i) => {
      // ensure bounds don't extend past -180,-90,180,90
      let coord = parseFloat(x).toFixed(2)
      if (i % 2 === 0) { // lon
        if (coord > 180) {
          coord = 180
        } else if (coord < -180) {
          coord = -180
        }
      } else { // lat
        if (coord > 90) {
          coord = 90
        } else if (coord < -90) {
          coord = -90
        }
      }
      return coord
    })
    bbox.value = bboxSplit.join(',')
  }

  // map initialize
  const setupMap = function() {
    map = L.map(mapElemId, {
      zoomDelta: 0.25,
      zoomSnap: 0.25
    }).setView([45, -75], 5)
    map.addLayer(new L.TileLayer(
      tileLayerUrl, {
        minZoom: 2,
        maxZoom: maxZoom,
        attribution: tileLayerAttr
      }
    ))
    layerItems = new L.GeoJSON({type: 'FeatureCollection', features: []})
    map.addLayer(layerItems)
    // update bbox for permalink feature
    map.on('moveend', updateBboxEvent)
  }
  onMounted(setupMap)

  // update map with new geoJson data
  watch(geoJsonData, (newData, oldData) => {
    if (map.hasLayer(layerItems)) {
      map.removeLayer(layerItems)
    }
    
    // feature collection
    if (Object.prototype.hasOwnProperty.call(geoJsonData.value, 'features')) {
      if (geoJsonData.value.features.length === 0 || geoJsonData.value.features[0].geometry === null) {
        map.setView([0, 0], 1)
        return false
      }
    } else if (geoJsonData.value.type === 'Feature') { // single feature
      if (geoJsonData.value.geometry === null) {
        map.setView([0, 0], 1)
        return false
      }
    }
    
    layerItems = new L.GeoJSON(geoJsonData.value, {
      onEachFeature: function (feature, layer) {
        let url = itemsPath + '/' + feature.id + `?lang=${locale}`
        let html = '<span><a href="' + url + '">' + feature.id + '</a></span>'
        layer.bindPopup(html)
      }
    })
    map.addLayer(layerItems)
    if (bbox.value === '' || !bboxPermalink.value) { // no bbox
      map.fitBounds(layerItems.getBounds(), {maxZoom: 5})
    } else if (Object.keys(oldData).length === 0) { // first time load
      // don't trigger moveend event after a fitBounds() to avoid overwriting original BBOX from permalink
      map.off('moveend', updateBboxEvent) // temporarily remove moveend event
      map.fitBounds(bboxLatLngBounds.value, {maxZoom: maxZoom})
      setTimeout(() => { // add moveend event back after short delay
        map.on('moveend', updateBboxEvent)
      }, 2000)
    }
  })

  return {
    setupMap, bbox
  }
}