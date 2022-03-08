import { ref, computed } from 'https://cdnjs.cloudflare.com/ajax/libs/vue/3.0.7/vue.esm-browser.prod.js'

export default function useItems() {
  // Items results
  const itemsLoading = ref(false)
  const itemsJson = ref({})
  const itemProps = ref([])
  const itemsTotal = computed(() => {
    if (Object.prototype.hasOwnProperty.call(itemsJson.value, 'numberMatched')) {
      return itemsJson.value.numberMatched
    } else {
      return 500
    }
  })
  const items = computed(() => {
    if (Object.prototype.hasOwnProperty.call(itemsJson.value, 'features')) {
      // map items to only show its properties
      return itemsJson.value.features.map((item) => {
        return {
          id: item.id, // include root "id"
          ...item.properties
        }
      })
    } else {
      return []
    }
  })

  // Pagination
  const limit = ref(10) // default
  const currentPage = ref(1)
  const showingLimitText = computed(() => {
    let upper = parseInt(offset.value) + parseInt(limit.value)
    if (upper >= itemsTotal.value) {
      upper = itemsTotal.value
    }
    const firstPage = parseInt(offset.value) + 1
    let showText = `Showing ${firstPage} to ${upper} of ${itemsTotal.value}`
    if (upper === 0) {
      showText = 'Showing 0 results'
    }
    return showText
  })
  const calcOffset = () => {
    if (currentPage.value === 1) {
      return 0
    } else {
      // - 1 for lower range from current page
      const index = Math.floor((currentPage.value - 1) * limit.value)
      if (index < limit.value) {
        return 0
      } else {
        return index
      }
    }
  }
  const offset = computed(() => {
    return calcOffset()
  })
  const maxPages = computed(() => {
    const max = Math.ceil(itemsTotal.value / limit.value)
    if (max === 0) {
      return 1
    }
    return max
  })
  const nextPage = () => {
    if ((currentPage.value * limit.value) < itemsTotal.value) {
      currentPage.value++
    }
  }
  const prevPage = () => {
    if (currentPage.value > 1) {
      currentPage.value--
    }
  }

  // Data retrieval
  const requestUrl = ref('')
  const queryCols = ref({}) // optional querying per column
  const queryColsIsEmpty = computed(() => {
    let isEmpty = true
    for (const [key, value] of Object.entries(queryCols.value)) {
      if (value !== '') {
        isEmpty = false
        break;
      }
    }
    return isEmpty
  })
  const setTableHeaders = (dataJson) => {
    if (itemProps.value.length === 0 && dataJson.features.length > 0) { // initalize itemProps once from JSON data
      // use first row for list of keys/properties
      itemProps.value = Object.keys(dataJson.features[0].properties)
    }
  }
  const getItems = async (sortCol = '', sortDir = '', bbox = '') => {
    // Request URL
    let newRequestUrl = `${window.location.pathname}?f=json&limit=${limit.value}&offset=${offset.value}`  // relative to /items

    // Query params
    let newQueryColValues = []
    for (const [key, value] of Object.entries(queryCols.value)) {
      if (value !== '') {
        newQueryColValues.push(key + '=' + value)
      }
    }
    if (newQueryColValues.length > 0) {
      newRequestUrl += '&' + newQueryColValues.join('&')
    }

    // Optional sort
    if (sortCol !== '' && sortDir !== '') {
      if (sortCol !== 'id') { // root id is an internal column
        newRequestUrl += '&sortby=' + (sortDir === 'desc' ? '-' : '') + sortCol
      }
    }

    // Optional bbox
    if (bbox !== '') {
      newRequestUrl += '&bbox=' + bbox
    }

    if (requestUrl.value === newRequestUrl) {
      return false // prevent duplicate calls
    }

    try {
      itemsLoading.value = true
      requestUrl.value = newRequestUrl
      const resp = await axios.get(requestUrl.value)
      itemsJson.value = resp.data // original JSON data
      setTableHeaders(itemsJson.value)
      itemsLoading.value = false
    } catch (err) {
      console.error(err)
      itemsLoading.value = false
    }
  }
  // Populate table headers by retrieving 1 valid result
  const getTableHeaders = async () => {
    // Request URL
    let newRequestUrl = `${window.location.pathname}?f=json&limit=1`  // relative to /items

    try {
      const resp = await axios.get(newRequestUrl)
      const data = resp.data
      setTableHeaders(data)
    } catch (err) {
      console.error(err)
    }
  }

  const clearQueryCols = () => {
    for (const [key] of Object.entries(queryCols.value)) {
      queryCols.value[key] = ''
    }
  }

  return {
    itemsJson, itemsTotal, items, itemProps, limit,
    getItems, getTableHeaders, showingLimitText, itemsLoading,
    nextPage, prevPage, currentPage, maxPages, calcOffset,
    queryCols, clearQueryCols, queryColsIsEmpty
  }
}
