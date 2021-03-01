/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "spdk/mmio.h"
#include "spdk/nvme_spec.h"
#include "spdk/log.h"
#include "spdk/stdinc.h"
#include "nvme.h"

struct nvme_request {
	/* Command identifier and position within qpair's requests array */
	uint16_t			cid;
	/* NVMe command */
	struct spdk_nvme_cmd		cmd;
	TAILQ_ENTRY(nvme_request)	tailq;
};

struct nvme_qpair {
	/* Submission queue */
	struct spdk_nvme_cmd		*cmd;
	/* Completion queue */
	struct spdk_nvme_cpl		*cpl;
	/* Physical address of the submission queue */
	uint64_t			sq_paddr;
	/* Physical address of the completion queue */
	uint64_t			cq_paddr;
	/* Submission queue tail doorbell */
	volatile uint32_t		*sq_tdbl;
	/* Completion queue head doorbell */
	volatile uint32_t		*cq_hdbl;
	/* Submission/completion queues pointers */
	uint16_t			sq_head;
	uint16_t			sq_tail;
	uint16_t			cq_head;
	/* Current phase tag value */
	uint8_t				phase;
	/* NVMe requests queue */
	TAILQ_HEAD(, nvme_request)	free_requests;
	struct nvme_request		*requests;
	/* Size of both queues */
	uint32_t			num_entries;
};

enum nvme_ctrlr_state {
	/* Controller has not been initialized yet */
	NVME_CTRLR_STATE_INIT,
	/* Waiting for CSTS.RDY to transition from 0 to 1 so that CC.EN may be set to 0 */
	NVME_CTRLR_STATE_DISABLE_WAIT_FOR_READY_1,
	/* Waiting for CSTS.RDY to transition from 1 to 0 so that CC.EN may be set to 1 */
	NVME_CTRLR_STATE_DISABLE_WAIT_FOR_READY_0,
	/* Enable the controller by writing CC.EN to 1 */
	NVME_CTRLR_STATE_ENABLE,
	/* Waiting for CSTS.RDY to transition from 0 to 1 after enabling the controller */
	NVME_CTRLR_STATE_ENABLE_WAIT_FOR_READY_1,
	/* Identify Controller command will be sent to then controller */
	NVME_CTRLR_STATE_IDENTIFY,
	/* Waiting for Identify Controller command to be completed */
	NVME_CTRLR_STATE_WAIT_FOR_IDENTIFY,
	/* Controller initialization has completed and the controller is ready */
	NVME_CTRLR_STATE_READY,
	/*  Controller initialization error */
	NVME_CTRLR_STATE_ERROR,
};

struct nvme_ctrlr {
	/* Underlying PCI device */
	struct spdk_pci_device			*pci_device;
	/* Pointer to the MMIO register space */
	volatile struct spdk_nvme_registers	*regs;
	/* Stride in uint32_t units between doorbells */
	uint32_t				doorbell_stride_u32;
	/* Controller's memory page size */
	uint32_t				page_size;
	/* Admin queue pair */
	struct nvme_qpair			*admin_qpair;
	/* State of the controller */
	enum nvme_ctrlr_state			state;
	TAILQ_ENTRY(nvme_ctrlr)			tailq;
};

static struct spdk_pci_id nvme_pci_driver_id[] = {
	{
		.class_id = SPDK_PCI_CLASS_NVME,
		.vendor_id = SPDK_PCI_ANY_ID,
		.device_id = SPDK_PCI_ANY_ID,
		.subvendor_id = SPDK_PCI_ANY_ID,
		.subdevice_id = SPDK_PCI_ANY_ID,
	},
	{ .vendor_id = 0, /* sentinel */ },
};

SPDK_PCI_DRIVER_REGISTER(nvme_external, nvme_pci_driver_id, SPDK_PCI_DRIVER_NEED_MAPPING);
static TAILQ_HEAD(, nvme_ctrlr) g_nvme_ctrlrs = TAILQ_HEAD_INITIALIZER(g_nvme_ctrlrs);

static struct nvme_ctrlr *
find_ctrlr_by_addr(struct spdk_pci_addr *addr)
{
	struct spdk_pci_addr ctrlr_addr;
	struct nvme_ctrlr *ctrlr;

	TAILQ_FOREACH(ctrlr, &g_nvme_ctrlrs, tailq) {
		ctrlr_addr = spdk_pci_device_get_addr(ctrlr->pci_device);
		if (spdk_pci_addr_compare(addr, &ctrlr_addr) == 0) {
			return ctrlr;
		}
	}

	return NULL;
}

static volatile void *
get_pcie_reg_addr(struct nvme_ctrlr *ctrlr, uint32_t offset)
{
	return (volatile void *)((uintptr_t)ctrlr->regs + offset);
}

static void
get_pcie_reg_8(struct nvme_ctrlr *ctrlr, uint32_t offset, uint64_t *value)
{
	assert(offset <= sizeof(struct spdk_nvme_registers) - 8);
	*value = spdk_mmio_read_8(get_pcie_reg_addr(ctrlr, offset));
}

static void
get_pcie_reg_4(struct nvme_ctrlr *ctrlr, uint32_t offset, uint32_t *value)
{
	assert(offset <= sizeof(struct spdk_nvme_registers) - 4);
	*value = spdk_mmio_read_4(get_pcie_reg_addr(ctrlr, offset));
}

static void
nvme_ctrlr_get_cap(struct nvme_ctrlr *ctrlr, union spdk_nvme_cap_register *cap)
{
	get_pcie_reg_8(ctrlr, offsetof(struct spdk_nvme_registers, cap), &cap->raw);
}

static void
nvme_ctrlr_get_cc(struct nvme_ctrlr *ctrlr, union spdk_nvme_cc_register *cc)
{
	get_pcie_reg_4(ctrlr, offsetof(struct spdk_nvme_registers, cc), &cc->raw);
}

static void
nvme_ctrlr_get_csts(struct nvme_ctrlr *ctrlr, union spdk_nvme_csts_register *csts)
{
	get_pcie_reg_4(ctrlr, offsetof(struct spdk_nvme_registers, csts), &csts->raw);
}

static int
nvme_ctrlr_allocate_bars(struct nvme_ctrlr *ctrlr)
{
	uint64_t phys_addr, size;
	void *addr;
	int rc;

	rc = spdk_pci_device_map_bar(ctrlr->pci_device, 0, &addr, &phys_addr, &size);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to map BAR 0\n");
		return rc;
	}

	ctrlr->regs = (volatile struct spdk_nvme_registers *)addr;
	return 0;
}

static void
free_qpair(struct nvme_qpair *qpair)
{
	spdk_free(qpair->cmd);
	spdk_free(qpair->cpl);
	free(qpair->requests);
	free(qpair);
}

static struct nvme_qpair *
init_qpair(struct nvme_ctrlr *ctrlr, uint16_t id, uint16_t num_entries)
{
	struct nvme_qpair *qpair;
	size_t page_align = sysconf(_SC_PAGESIZE);
	size_t queue_align, queue_len;
	volatile uint32_t *doorbell_base;
	uint16_t i;

	qpair = calloc(1, sizeof(*qpair));
	if (!qpair) {
		SPDK_ERRLOG("Failed to allocate queue pair\n");
		return NULL;
	}

	qpair->phase = 1;
	qpair->num_entries = num_entries;
	queue_len = num_entries * sizeof(struct spdk_nvme_cmd);
	queue_align = spdk_max(spdk_align32pow2(queue_len), page_align);
	qpair->cmd = spdk_zmalloc(queue_len, queue_align, NULL,
				  SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
	if (!qpair->cmd) {
		SPDK_ERRLOG("Failed to allocate submission queue buffer\n");
		free_qpair(qpair);
		return NULL;
	}

	queue_len = num_entries * sizeof(struct spdk_nvme_cpl);
	queue_align = spdk_max(spdk_align32pow2(queue_len), page_align);
	qpair->cpl = spdk_zmalloc(queue_len, queue_align, NULL,
				  SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
	if (!qpair->cpl) {
		SPDK_ERRLOG("Failed to allocate completion queue buffer\n");
		free_qpair(qpair);
		return NULL;
	}

	qpair->requests = calloc(num_entries - 1, sizeof(*qpair->requests));
	if (!qpair->requests) {
		SPDK_ERRLOG("Failed to allocate NVMe request descriptors\n");
		free_qpair(qpair);
		return NULL;
	}

	TAILQ_INIT(&qpair->free_requests);
	for (i = 0; i < num_entries - 1; ++i) {
		qpair->requests[i].cid = i;
		TAILQ_INSERT_TAIL(&qpair->free_requests, &qpair->requests[i], tailq);
	}

	qpair->sq_paddr = spdk_vtophys(qpair->cmd, NULL);
	qpair->cq_paddr = spdk_vtophys(qpair->cpl, NULL);
	if (qpair->sq_paddr == SPDK_VTOPHYS_ERROR || qpair->cq_paddr == SPDK_VTOPHYS_ERROR) {
		SPDK_ERRLOG("Failed to translate the sq/cq virtual address\n");
		free_qpair(qpair);
		return NULL;
	}

	doorbell_base = (volatile uint32_t *)&ctrlr->regs->doorbell[0];
	qpair->sq_tdbl = doorbell_base + (2 * id + 0) * ctrlr->doorbell_stride_u32;
	qpair->cq_hdbl = doorbell_base + (2 * id + 1) * ctrlr->doorbell_stride_u32;

	return qpair;
}

static int
pcie_nvme_enum_cb(void *ctx, struct spdk_pci_device *pci_dev)
{
	struct nvme_ctrlr *ctrlr;
	TAILQ_HEAD(, nvme_ctrlr) *ctrlrs = ctx;
	union spdk_nvme_cap_register cap;
	uint16_t cmd_reg;
	char addr[64] = {};

	spdk_pci_addr_fmt(addr, sizeof(addr), &pci_dev->addr);

	ctrlr = calloc(1, sizeof(*ctrlr));
	if (!ctrlr) {
		SPDK_ERRLOG("Failed to allocate NVMe controller: %s\n", addr);
		return -1;
	}

	ctrlr->pci_device = pci_dev;
	if (spdk_pci_device_claim(pci_dev)) {
		SPDK_ERRLOG("Failed to claim PCI device: %s\n", addr);
		free(ctrlr);
		return -1;
	}

	if (nvme_ctrlr_allocate_bars(ctrlr)) {
		SPDK_ERRLOG("Failed to allocate BARs for NVMe controller: %s\n", addr);
		spdk_pci_device_unclaim(pci_dev);
		free(ctrlr);
		return -1;
	}

	/* Enable PCI busmaster and disable INTx */
	spdk_pci_device_cfg_read16(pci_dev, &cmd_reg, 4);
	cmd_reg |= 0x404;
	spdk_pci_device_cfg_write16(pci_dev, cmd_reg, 4);

	nvme_ctrlr_get_cap(ctrlr, &cap);
	ctrlr->page_size = 1 << (12 + cap.bits.mpsmin);
	ctrlr->doorbell_stride_u32 = 1 << cap.bits.dstrd;

	ctrlr->admin_qpair = init_qpair(ctrlr, 0, 2);
	if (!ctrlr->admin_qpair) {
		SPDK_ERRLOG("Failed to initialize admin queue pair for controller: %s\n", addr);
		spdk_pci_device_unclaim(pci_dev);
		free(ctrlr);
		return -1;
	}

	TAILQ_INSERT_TAIL(ctrlrs, ctrlr, tailq);

	return 0;
}

static int
nvme_ctrlr_process_init(struct nvme_ctrlr *ctrlr)
{
	union spdk_nvme_cc_register cc;
	union spdk_nvme_csts_register csts;

	nvme_ctrlr_get_cc(ctrlr, &cc);
	nvme_ctrlr_get_csts(ctrlr, &csts);

	/* Immediately mark the controller as ready for now */
	ctrlr->state = NVME_CTRLR_STATE_READY;

	return 0;
}

static void
nvme_ctrlr_free(struct nvme_ctrlr *ctrlr)
{
	spdk_pci_device_unclaim(ctrlr->pci_device);
	spdk_pci_device_detach(ctrlr->pci_device);
	free_qpair(ctrlr->admin_qpair);
	free(ctrlr);
}

static int
probe_internal(struct spdk_pci_addr *addr, nvme_attach_cb attach_cb, void *cb_ctx)
{
	struct nvme_ctrlr *ctrlr, *tmp;
	TAILQ_HEAD(, nvme_ctrlr) ctrlrs = TAILQ_HEAD_INITIALIZER(ctrlrs);
	int rc, init_complete;

	if (addr == NULL) {
		rc = spdk_pci_enumerate(spdk_pci_get_driver("nvme_external"),
					pcie_nvme_enum_cb, &ctrlrs);
	} else {
		rc = spdk_pci_device_attach(spdk_pci_get_driver("nvme_external"),
					    pcie_nvme_enum_cb, &ctrlrs, addr);
	}

	if (rc != 0) {
		SPDK_ERRLOG("Failed to enumerate PCI devices\n");
		while (!TAILQ_EMPTY(&ctrlrs)) {
			ctrlr = TAILQ_FIRST(&ctrlrs);
			TAILQ_REMOVE(&ctrlrs, ctrlr, tailq);
			nvme_ctrlr_free(ctrlr);
		}

		return rc;
	}

	do {
		init_complete = 1;
		TAILQ_FOREACH_SAFE(ctrlr, &ctrlrs, tailq, tmp) {
			rc = nvme_ctrlr_process_init(ctrlr);
			if (rc != 0) {
				SPDK_ERRLOG("NVMe controller initialization failed\n");
				TAILQ_REMOVE(&ctrlrs, ctrlr, tailq);
				nvme_ctrlr_free(ctrlr);
				continue;
			}

			if (ctrlr->state != NVME_CTRLR_STATE_READY) {
				init_complete = 0;
			}
		}
	} while (!init_complete);

	TAILQ_FOREACH_SAFE(ctrlr, &ctrlrs, tailq, tmp) {
		TAILQ_REMOVE(&ctrlrs, ctrlr, tailq);
		TAILQ_INSERT_TAIL(&g_nvme_ctrlrs, ctrlr, tailq);

		if (attach_cb != NULL) {
			attach_cb(cb_ctx, &ctrlr->pci_device->addr, ctrlr);
		}
	}

	return 0;
}

int
nvme_probe(nvme_attach_cb attach_cb, void *cb_ctx)
{
	return probe_internal(NULL, attach_cb, cb_ctx);
}

struct nvme_ctrlr *
nvme_connect(struct spdk_pci_addr *addr)
{
	int rc;

	rc = probe_internal(addr, NULL, NULL);
	if (rc != 0) {
		return NULL;
	}

	return find_ctrlr_by_addr(addr);
}

void
nvme_detach(struct nvme_ctrlr *ctrlr)
{
	TAILQ_REMOVE(&g_nvme_ctrlrs, ctrlr, tailq);
	nvme_ctrlr_free(ctrlr);
}

SPDK_LOG_REGISTER_COMPONENT(nvme_external)
