import { ref, computed } from 'https://cdnjs.cloudflare.com/ajax/libs/vue/3.0.7/vue.esm-browser.prod.js'

export default function useCatalog() {
  const catalogJson = ref(JSON_DATA)
  const linksTotal = computed(() => {
    return links.value.length
  })
  const links = computed(() => {
    if (Object.prototype.hasOwnProperty.call(catalogJson.value, 'links')) {
      return catalogJson.value.links
    } else {
      return []
    }
  })
  const childLinks = computed(() => {
    let children = []
    links.value.forEach(link => {
      if (link.rel === 'child') {
        const splitLink = link.href.split('/')
        link.name = splitLink[splitLink.length-1] // last path
        children.push(link)
      }
    })
    return children
  })
  const itemLinks = computed(() => {
    let children = []
    links.value.forEach(link => {
      if (link.rel === 'item') {
        if (Object.prototype.hasOwnProperty.call(link, 'title')) { // null title check for older stac json
          link.name = link.title
        } else {
          link.name = link.href
        }
        children.push(link)
      }
    })
    return children
  })

  return {
    links, childLinks, itemLinks,
    linksTotal,
    catalogJson
  }
}