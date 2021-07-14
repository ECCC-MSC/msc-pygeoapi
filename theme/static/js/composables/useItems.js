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
          id: item.id, // include default "id"
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
    let upper = parseInt(startindex.value) + parseInt(limit.value)
    if (upper >= itemsTotal.value) {
      upper = itemsTotal.value
    }
    let showText = `Showing ${parseInt(startindex.value) + 1} to ${upper} of ${itemsTotal.value}`
    return showText
  })
  const calcStartIndex = () => {
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
  const startindex = computed(() => {
    return calcStartIndex()
  })
  const maxPages = computed(() => {
    return Math.ceil(itemsTotal.value / limit.value)
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
  const getItems = async (sortCol = '', sortDir = '', bbox = '') => {
    // Request URL
    let newRequestUrl = `${window.location.pathname}?f=json&limit=${limit.value}&startindex=${startindex.value}`  // relative to /items

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
      if (sortCol !== 'id') { // id is an internal column
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
      if (itemProps.value.length === 0) { // initalize itemProps once from JSON data
        itemProps.value = Object.keys(items.value[0])
      }
      itemsLoading.value = false
    } catch (err) {
      console.error(err)
      itemsLoading.value = false
    }
  }

  const clearQueryCols = () => {
    for (const [key] of Object.entries(queryCols.value)) {
      queryCols.value[key] = ''
    }
  }

  return {
    itemsJson, itemsTotal, items, itemProps, limit,
    getItems, showingLimitText, itemsLoading,
    nextPage, prevPage, currentPage, maxPages, calcStartIndex,
    queryCols, clearQueryCols, queryColsIsEmpty
  }
}